/*
  Copyright (c) 2012-2013 DataLab, S.L. <http://www.datalab.es>

  This file is part of the cluster/ida translator for GlusterFS.

  The cluster/ida translator for GlusterFS is free software: you can
  redistribute it and/or modify it under the terms of the GNU General
  Public License as published by the Free Software Foundation, either
  version 3 of the License, or (at your option) any later version.

  The cluster/ida translator for GlusterFS is distributed in the hope
  that it will be useful, but WITHOUT ANY WARRANTY; without even the
  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
  PURPOSE. See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with the cluster/ida translator for GlusterFS. If not, see
  <http://www.gnu.org/licenses/>.
*/

#include "gfsys.h"

#include "ida-common.h"
#include "ida-type-dict.h"
#include "ida-manager.h"
#include "ida-rabin.h"
#include "ida-mem-types.h"
#include "ida-heal.h"
#include "ida.h"

void ida_request_destroy(ida_request_t * req)
{
    ida_answer_t * ans, * tmp, * next;

    STACK_DESTROY(req->rframe->root);

    sys_loc_release(&req->loc1);
    sys_loc_release(&req->loc2);
    sys_fd_release(req->fd);

    list_for_each_entry_safe(ans, tmp, &req->answers, list)
    {
        list_del_init(&ans->list);
        while (ans->next != NULL)
        {
            next = ans->next;
            ans->next = next->next;
            sys_gf_args_free((uintptr_t *)next);
        }
        sys_gf_args_free((uintptr_t *)ans);
    }

    sys_gf_args_free((uintptr_t *)req);
}

void ida_complete(ida_request_t * req)
{
    ida_private_t * ida;
    ida_answer_t * ans;
    uintptr_t mask;
    err_t error;

    dfc_complete(req->txn);
    if (atomic_dec(&req->pending, memory_order_seq_cst) == 1)
    {
        ans = list_entry(req->answers.next, ida_answer_t, list);
        if (req->completed == 0)
        {
            req->completed = 1;
            ida = req->xl->private;
            error = EIO;
            if (ans->count >= req->minimum)
            {
                if (req->handlers->rebuild(ida, req, ans) >= 0)
                {
                    error = 0;
                }
            }

            req->handlers->completed(req->frame, error, req,
                                     (uintptr_t *)ans + IDA_ANS_SIZE);
        }

        mask = req->sent & ~ans->mask;
        if (mask != 0)
        {
            ida_heal(req->xl, &req->loc1, &req->loc2, req->fd);
        }

        ida_request_destroy(req);
    }
}

void ida_unwind(ida_request_t * req, err_t error, uintptr_t * data)
{
    if (atomic_xchg(&req->completed, 1, memory_order_seq_cst) == 0)
    {
        req->handlers->completed(req->frame, error, req, data);
    }
}

SYS_LOCK_CREATE(__ida_dispatch_cbk, ((uintptr_t *, io),
                                     (ida_private_t *, ida),
                                     (ida_request_t *, req),
                                     (uint32_t, id)))
{
    ida_answer_t * ans, * tmp, * final;
    struct list_head * item;
    int32_t needed, ret;

    list_for_each_entry(ans, &req->answers, list)
    {
        if (req->handlers->combine(req, id, ans, io))
        {
            ans->count++;
            ans->mask |= 1ULL << id;

            item = ans->list.prev;
            while (item != &req->answers)
            {
                tmp = list_entry(item, ida_answer_t, list);
                if (tmp->count >= ans->count)
                {
                    break;
                }
                item = item->prev;
            }
            list_del(&ans->list);
            list_add(&ans->list, item);

            goto merged;
        }
        else
        {
            logW("IDA: combine failed for request %ld", req->txn->id);
        }
    }

    ans = req->handlers->copy(io);
    ans->count = 1;
    ans->mask = 1ULL << id;
    ans->id = id;
    ans->next = NULL;
    list_add_tail(&ans->list, &req->answers);

merged:
    final = NULL;
    if (ans->count == req->required)
    {
        final = req->handlers->copy((uintptr_t *)ans + IDA_ANS_SIZE);
        final->count = ans->count;
        final->mask = ans->mask;
        final->id = ans->id;
        final->next = ans->next;
    }

    tmp = list_entry(req->answers.next, ida_answer_t, list);
    needed = SYS_MIN(req->required, req->minimum) - tmp->count -
             req->pending + 1;
    if (needed > 0)
    {
        req->failed |= req->last_sent & ~ tmp->mask;
    }

    SYS_UNLOCK(&req->lock);

    if (needed > 0)
    {
        req->handlers->dispatch(ida, req);
    }
    else if (final != NULL)
    {
        ret = req->handlers->rebuild(ida, req, final);
        if (ret >= 0)
        {
            ida_unwind(req, 0, (uintptr_t *)final + IDA_ANS_SIZE);
        }
        else
        {
            logE("IDA: rebuild failed");
            ida_unwind(req, EIO, (uintptr_t *)final + IDA_ANS_SIZE);
        }
        sys_gf_args_free((uintptr_t *)final);
    }

    ida_complete(req);
}

SYS_CBK_CREATE(ida_dispatch_cbk, io, ((ida_private_t *, ida),
                                      (ida_request_t *, req),
                                      (uint32_t, id)))
{
    SYS_GF_WIND_CBK_TYPE(access) * args;

    args = (SYS_GF_WIND_CBK_TYPE(access) *)io;
    if ((args->op_ret >= 0) || (args->op_errno != EUCLEAN))
    {
        SYS_LOCK(&req->lock, __ida_dispatch_cbk, (io, ida, req, id));
    }
    else
    {
        req->handlers->dispatch(ida, req);

        ida_complete(req);
    }
}

int32_t ida_get_childs(ida_private_t * ida, int32_t count, uintptr_t * mask)
{
    int32_t first, idx, num;
    uintptr_t map1, map2;

    // This is not thread-safe, but its only purpose is to balance requests.
    // If we lose some increments, it's not a problem.
    idx = first = ida->index;
    if (++first >= ida->nodes)
    {
        first = 0;
    }
    ida->index = first;
    map2 = *mask;
    map1 = map2 & ((1ULL << idx) - 1ULL);
    num = sys_bits_count64(map1);
    if (count <= num)
    {
        map2 = 0;
        while (count < num)
        {
            map1 ^= sys_bits_first_one_mask64(map1);
            num--;
        }
    }
    else
    {
        map2 &= ~((1ULL << idx) - 1ULL);
        num += sys_bits_count64(map2);
        while (count < num)
        {
            map2 ^= sys_bits_first_one_mask64(map2);
            num--;
        }
    }
    *mask = map1 | map2;

    return num;
}

void ida_dispatch_incremental(ida_private_t * ida, ida_request_t * req)
{
    uintptr_t mask;
    int32_t idx;

    mask = ida->xl_up & ~req->sent & ~req->bad;
    if (ida_get_childs(ida, 1, &mask) > 0)
    {
        if (req->txn == IDA_USE_DFC)
        {
            SYS_CALL(
                dfc_begin, (ida->dfc, mask, NULL, *req->xdata, &req->txn),
                E(),
                GOTO(failed)
            );
        }

        req->last_sent = mask;
        req->sent |= mask;
        idx = sys_bits_first_one_index64(mask);
        SYS_CALL(
            dfc_attach, (req->txn, idx, req->xdata),
            E(),
            GOTO(failed)
        );

        atomic_inc(&req->pending, memory_order_seq_cst);
        sys_gf_wind(req->rframe, NULL, ida->xl_list[idx],
                    SYS_CBK(ida_dispatch_cbk, (ida, req, idx)),
                    NULL, (uintptr_t *)req, (uintptr_t *)req + IDA_REQ_SIZE);

        return;
    }

failed:
    logE("IDA: incremental dispatch failed");
    ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);
}

void ida_dispatch_all(ida_private_t * ida, ida_request_t * req)
{
    uintptr_t mask;
    int32_t idx, count;

    SYS_TEST(
        req->sent == 0,
        EIO,
        D(),
        GOTO(failed)
    );

    mask = ida->xl_up & ~req->bad;
    count = sys_bits_count64(mask);
    if (count >= req->minimum)
    {
        if (req->txn == IDA_USE_DFC)
        {
            SYS_CALL(
                dfc_begin, (ida->dfc, mask, NULL, *req->xdata, &req->txn),
                E(),
                GOTO(failed)
            );
        }
        atomic_add(&req->pending, count, memory_order_seq_cst);
        req->last_sent = req->sent = mask;
        do
        {
            idx = sys_bits_first_one_index64(mask);
            mask ^= 1ULL << idx;
            SYS_CALL(
                dfc_attach, (req->txn, idx, req->xdata),
                E(),
                CONTINUE()
            );
            count--;
            sys_gf_wind(req->rframe, NULL, ida->xl_list[idx],
                        SYS_CBK(ida_dispatch_cbk, (ida, req, idx)),
                        NULL, (uintptr_t *)req,
                        (uintptr_t *)req + IDA_REQ_SIZE);
        } while (mask != 0);
        if (count > 0)
        {
            dfc_failed(req->txn, count);
        }

        return;
    }

    logE("IDA: dispatch to all failed");

    ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);

    return;

failed:
    ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);
}

void ida_dispatch_minimum(ida_private_t * ida, ida_request_t * req)
{
    uintptr_t mask;
    int32_t idx, i, count;

    if ((req->sent != 0) && (req->txn == IDA_SKIP_DFC))
    {
        ida_dispatch_incremental(ida, req);

        return;
    }

    mask = ida->xl_up & ~req->failed & ~req->bad;
    count = ida_get_childs(ida, req->required, &mask);
    if (count >= req->minimum)
    {
        if (req->txn == IDA_USE_DFC)
        {
            SYS_CALL(
                dfc_begin, (ida->dfc, mask, NULL, *req->xdata, &req->txn),
                E(),
                GOTO(failed)
            );
        }
        atomic_add(&req->pending, count, memory_order_seq_cst);
        req->last_sent = mask;
        req->sent |= mask;
        i = 0;
        do
        {
            idx = sys_bits_first_one_index64(mask);
            mask ^= 1ULL << idx;
            SYS_CALL(
                dfc_attach, (req->txn, idx, req->xdata),
                E(),
                CONTINUE()
            );
            sys_gf_wind(req->rframe, NULL, ida->xl_list[idx],
                        SYS_CBK(ida_dispatch_cbk, (ida, req, idx)),
                        NULL, (uintptr_t *)req,
                        (uintptr_t *)req + IDA_REQ_SIZE);
            i++;
        } while ((i < count) && (mask != 0));
        if (i < count)
        {
            dfc_failed(req->txn, count - i);
        }

        return;
    }

    ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);

    return;

failed:
    ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);
}

SYS_CBK_CREATE(ida_dispatch_write_cbk, io, ((ida_private_t *, ida),
                                            (ida_request_t *, req),
                                            (uint32_t, id)))
{
    SYS_GF_WIND_CBK_TYPE(writev) * args;

    args = (SYS_GF_WIND_CBK_TYPE(writev) *)io;
    if ((args->op_ret >= 0) || (args->op_errno != EUCLEAN))
    {
        SYS_LOCK(&req->lock, __ida_dispatch_cbk, (io, ida, req, id));
    }
    else
    {
        req->handlers->dispatch(ida, req);

        ida_complete(req);
    }
}

void __ida_dispatch_write(ida_private_t * ida, ida_request_t * req,
                          uint8_t * buffer, off_t offset, size_t size,
                          size_t head, size_t tail, uintptr_t mask)
{
    SYS_GF_FOP_CALL_TYPE(writev) * args;
    struct iobref * iobref;
    struct iobuf * iobuf;
    uint8_t * ptr;
    ssize_t remaining, slice, pagesize, maxsize;
    int32_t idx, i, j, count;

    if (atomic_dec(&req->data, memory_order_seq_cst) != 1)
    {
        return;
    }

    count = sys_bits_count64(mask);
    i = 0;

    SYS_TEST(
        req->flags == 0,
        EIO,
        E(),
        GOTO(failed)
    );

    args = (SYS_GF_FOP_CALL_TYPE(writev) *)((uintptr_t *)req + IDA_REQ_SIZE);

    ptr = buffer + head;
    for (i = 0; i < args->vector.count; i++)
    {
        memcpy(ptr, args->vector.iovec[i].iov_base,
               args->vector.iovec[i].iov_len);
        ptr += args->vector.iovec[i].iov_len;
    }

    req->size = head + tail;

    atomic_add(&req->pending, count, memory_order_seq_cst);
    req->last_sent = req->sent = mask;
    pagesize = iobpool_default_pagesize(
                                (struct iobuf_pool *)ida->xl->ctx->iobuf_pool);
    maxsize = pagesize * ida->fragments;
    i = 0;
    do
    {
        struct iovec vector[args->vector.count];

        idx = sys_bits_first_one_index64(mask);

        SYS_PTR(
            &iobref, iobref_new, (),
            ENOMEM,
            E(),
            GOTO(failed)
        );
        remaining = size;
        j = 0;
        ptr = buffer;
        do
        {
            SYS_PTR(
                &iobuf, iobuf_get, (ida->xl->ctx->iobuf_pool),
                ENOMEM,
                E(),
                GOTO(failed_iobref)
            );
            SYS_CODE(
                iobref_add, (iobref, iobuf),
                ENOMEM,
                E(),
                GOTO(failed_iobuf)
            );

            slice = remaining;
            if (slice > maxsize)
            {
                slice = maxsize;
            }
            ida_rabin_split(slice, ida->fragments, idx, ptr, iobuf->ptr);
            ptr += slice;

            vector[j].iov_base = iobuf->ptr;
            vector[j].iov_len = slice / ida->fragments;
            j++;

            iobuf_unref(iobuf);

            remaining -= slice;
        } while (remaining > 0);

        SYS_CALL(
            dfc_attach, (req->txn, idx, req->xdata),
            E(),
            GOTO(failed_iobuf)
        );
        SYS_IO(sys_gf_writev_wind, (req->rframe, NULL, ida->xl_list[idx],
                                    args->fd, vector, j,
                                    offset / ida->fragments, args->flags,
                                    iobref, *req->xdata),
               SYS_CBK(ida_dispatch_write_cbk, (ida, req, idx)));
        iobref_unref(iobref);

        mask ^= 1ULL << idx;
    } while (++i < count);

    if (i < count)
    {
        dfc_failed(req->txn, count - i);
    }

    SYS_FREE_ALIGNED(buffer);

    return;

failed_iobuf:
    iobuf_unref(iobuf);
failed_iobref:
    iobref_unref(iobref);
failed:
    dfc_failed(req->txn, count - i);
    logE("WRITE failed in __ida_dispatch_write");
    ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);

    SYS_FREE_ALIGNED(buffer);
}

SYS_CBK_CREATE(ida_dispatch_write_readv_cbk, io, ((ida_private_t *, ida),
                                                  (ida_request_t *, req),
                                                  (uint8_t *, buffer),
                                                  (uint8_t *, ptr),
                                                  (off_t, offset),
                                                  (size_t, size),
                                                  (size_t, head),
                                                  (size_t, tail),
                                                  (uintptr_t, mask)))
{
    SYS_GF_WIND_CBK_TYPE(readv) * args;

    args = (SYS_GF_WIND_CBK_TYPE(readv) *)io;
    if ((args->op_ret < 0) ||
        ((args->op_ret != 0) && (args->op_ret != ida->block_size)))
    {
        req->flags = 1;
    }
    else if (args->op_ret == 0)
    {
        memset(ptr, 0, ida->block_size);
    }
    else
    {
        memcpy(ptr, args->vector.iovec[0].iov_base, ida->block_size);
    }

    __ida_dispatch_write(ida, req, buffer, offset, size, head, tail, mask);
}

void ida_dispatch_write(ida_private_t * ida, ida_request_t * req)
{
    SYS_GF_FOP_CALL_TYPE(writev) * args;
    uint8_t * buffer;
    dict_t * xdata;
    off_t user_offs, offs;
    size_t user_size, size, head, tail, tmp;
    uintptr_t mask;
    int32_t count;

    SYS_TEST(
        req->sent == 0,
        EIO,
        D(),
        GOTO(failed)
    );

    mask = ida->xl_up & ~req->bad;
    count = sys_bits_count64(mask);
    SYS_TEST(
        count >= req->minimum,
        ENODATA,
        E(),
        GOTO(failed)
    );

    args = (SYS_GF_FOP_CALL_TYPE(writev) *)((uintptr_t *)req + IDA_REQ_SIZE);

    user_offs = args->offset;
    user_size = iov_length(args->vector.iovec, args->vector.count);

    head = user_offs % ida->block_size;
    offs = user_offs - head;
    size = user_size + head;
    tmp = ida->block_size - 1;
    tail = tmp - (size + tmp) % ida->block_size;
    size += tail;

    req->data = 1;
    if (req->minimum >= ida->fragments)
    {
        req->data += (head > 0) + ((tail > 0) && (size > ida->block_size));
    }

    req->flags = 0;
    SYS_ALLOC_ALIGNED(
        &buffer, size, 16, sys_mt_uint8_t,
        E(),
        GOTO(failed)
    );

    SYS_CALL(
        dfc_begin, (ida->dfc, mask, args->fd->inode, *req->xdata, &req->txn),
        E(),
        GOTO(failed_buffer)
    );

    if (head > 0)
    {
        if (req->minimum >= ida->fragments)
        {
            xdata = NULL;
            SYS_CALL(
                dfc_attach, (req->txn, 0, &xdata),
                E(),
                GOTO(failed_dfc)
            );
            SYS_IO(sys_gf_readv_wind, (req->rframe, NULL, ida->xl, args->fd,
                                       ida->block_size, offs, 0, xdata),
                   SYS_CBK(ida_dispatch_write_readv_cbk, (ida, req, buffer,
                                                          buffer, offs, size,
                                                          head, tail, mask)
                          ));
            sys_dict_release(xdata);
        }
        else
        {
            memset(buffer, 0, ida->block_size);
        }
    }

    if ((tail > 0) && (size > ida->block_size))
    {
        if (req->minimum >= ida->fragments)
        {
            xdata = NULL;
            SYS_CALL(
                dfc_attach, (req->txn, 0, &xdata),
                E(),
                GOTO(failed_dfc)
            );
            SYS_IO(sys_gf_readv_wind, (req->rframe, NULL, ida->xl, args->fd,
                                       ida->block_size,
                                       offs + size - ida->block_size, 0,
                                       xdata),
                   SYS_CBK(ida_dispatch_write_readv_cbk, (ida, req, buffer,
                                                          buffer + size -
                                                          ida->block_size,
                                                          offs, size, head,
                                                          tail, mask)
                          ));
            sys_dict_release(xdata);
        }
        else
        {
            memset(buffer + size - ida->block_size, 0, ida->block_size);
        }
    }

    __ida_dispatch_write(ida, req, buffer, offs, size, head, tail, mask);

    return;

failed_dfc:
    dfc_failed(req->txn, count);
failed_buffer:
    SYS_FREE(buffer);
failed:
    logE("WRITE failed in ida_dispatch_write");
    ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);
}
