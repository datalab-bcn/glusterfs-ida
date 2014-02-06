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
#include "ida.h"

void ida_complete(ida_request_t * req)
{
    ida_answer_t * ans, * tmp, * next;

    dfc_complete(req->txn);
    if (atomic_dec(&req->pending, memory_order_seq_cst) == 1)
    {
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
}

void ida_unwind(ida_request_t * req, err_t error, uintptr_t * data)
{
    if (atomic_xchg(&req->completed, 1, memory_order_seq_cst) == 0)
    {
        if (error == 0)
        {
            sys_gf_unwind(req->frame, 0, 0, NULL, NULL, (uintptr_t *)req,
                          data);
        }
        else
        {
            sys_gf_unwind_error(req->frame, error, NULL, NULL, NULL,
                                (uintptr_t *)req, data);
        }
    }
}

SYS_LOCK_CREATE(__ida_dispatch_cbk, ((uintptr_t *, io),
                                     (ida_private_t *, ida),
                                     (ida_request_t *, req),
                                     (uint32_t, id)))
{
    SYS_GF_CBK_CALL_TYPE(access) * args;
    ida_answer_t * ans, * tmp;
    struct list_head * item;
    int32_t needed, ret;

    list_for_each_entry(ans, &req->answers, list)
    {
        if (req->handlers->combine(req, id, ans, io))
        {
            ans->count++;

            item = ans->list.prev;
            while (item != &req->answers)
            {
                tmp = list_entry(item, ida_answer_t, list);
                if (tmp->count >= ans->count)
                {
                    break;
                }
                item = tmp->list.prev;
            }
            list_del(&ans->list);
            list_add(&ans->list, item);

            goto merged;
        }
        else
        {
            logW("IDA: combine failed");
        }
    }

    ans = req->handlers->copy(io);
    ans->count = 1;
    ans->id = id;
    ans->next = NULL;
    list_add_tail(&ans->list, &req->answers);

merged:
    if (ans->count == req->required)
    {
        args = (SYS_GF_CBK_CALL_TYPE(access) *)((uintptr_t *)ans +
                                                IDA_ANS_SIZE);
        ret = req->handlers->rebuild(ida, req, ans);
        if (ret >= 0)
        {
            ida_unwind(req, 0, (uintptr_t *)ans + IDA_ANS_SIZE);
        }
        else
        {
            logE("IDA: rebuild failed");
            args->op_ret = ret;
            ida_unwind(req, EIO, (uintptr_t *)ans + IDA_ANS_SIZE);
        }
    }

    tmp = list_entry(req->answers.next, ida_answer_t, list);
    needed = req->required - tmp->count - req->pending + 1;

    SYS_UNLOCK(&req->lock);

    if (needed > 0)
    {
        req->handlers->dispatch(ida, req);
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

void ida_dispatch_incremental(ida_private_t * ida, ida_request_t * req)
{
    uintptr_t mask;
    uint32_t idx;

    mask = ida->xl_up & ~req->sent;
    if (mask != 0)
    {
        idx = sys_bits_first_one_index64(mask);
        req->sent |= 1ULL << idx;
        atomic_inc(&req->pending, memory_order_seq_cst);
        sys_gf_wind(req->frame, NULL, ida->xl_list[idx],
                    SYS_CBK(ida_dispatch_cbk, (ida, req, idx)),
                    NULL, (uintptr_t *)req, (uintptr_t *)req + IDA_REQ_SIZE);
    }
    else
    {
        logE("IDA: incremental dispatch failed");
        ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);
    }
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

    mask = ida->xl_up;
    count = sys_bits_count64(mask);
    if (count >= ida->fragments)
    {
        atomic_add(&req->pending, count, memory_order_seq_cst);
        req->sent = mask;
        do
        {
            idx = sys_bits_first_one_index64(mask);
            sys_gf_wind(req->frame, NULL, ida->xl_list[idx],
                        SYS_CBK(ida_dispatch_cbk, (ida, req, idx)),
                        NULL, (uintptr_t *)req,
                        (uintptr_t *)req + IDA_REQ_SIZE);
            mask ^= 1ULL << idx;
        } while (mask != 0);
        dfc_end(req->txn, count);

        return;
    }

    dfc_end(req->txn, 0);
    logE("IDA: dispatch to all failed");

failed:
    ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);
}

void ida_dispatch_minimum(ida_private_t * ida, ida_request_t * req)
{
    uintptr_t mask;
    int32_t idx, i, count;

    // TODO: if request is DFC managed, we have to fully restart it.
    if (req->sent != 0)
    {
        ida_dispatch_incremental(ida, req);
    }
    else
    {
        mask = ida->xl_up;
        count = ida->fragments;
        if (sys_bits_count64(mask) >= count)
        {
            atomic_add(&req->pending, count, memory_order_seq_cst);
            i = 0;
            do
            {
                idx = sys_bits_first_one_index64(mask);
                req->sent |= 1ULL << idx;
                sys_gf_wind(req->frame, NULL, ida->xl_list[idx],
                            SYS_CBK(ida_dispatch_cbk, (ida, req, idx)),
                            NULL, (uintptr_t *)req,
                            (uintptr_t *)req + IDA_REQ_SIZE);
                mask ^= 1ULL << idx;
            } while (++i < count);
            dfc_end(req->txn, count);
        }
        else
        {
            dfc_end(req->txn, 0);
            logE("IDA: dispatch to minimum failed");
            ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);
        }
    }
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
                          size_t head, size_t tail)
{
    SYS_GF_FOP_CALL_TYPE(writev) * args;
    struct iovec vector[1];
    struct iobref * iobref;
    struct iobuf * iobuf;
    uintptr_t mask;
    int32_t idx, i, count;

    if (atomic_dec(&req->data, memory_order_seq_cst) != 1)
    {
        return;
    }

    SYS_TEST(
        req->flags == 0,
        EIO,
        E(),
        GOTO(failed)
    );

    mask = ida->xl_up;
    count = sys_bits_count64(mask);
    SYS_TEST(
        sys_bits_count64(mask) >= ida->fragments,
        EIO,
        E(),
        GOTO(failed)
    );

    args = (SYS_GF_FOP_CALL_TYPE(writev) *)((uintptr_t *)req + IDA_REQ_SIZE);

    memcpy(buffer + head, args->vector.iov_base, args->vector.iov_len);

    SYS_CALL(
        dfc_begin, (ida->dfc, &req->txn),
        E(),
        GOTO(failed)
    );
    SYS_CALL(
        dfc_attach, (req->txn, &args->xdata),
        E(),
        GOTO(failed_txn)
    );

    req->size = head + tail;

    atomic_add(&req->pending, count, memory_order_seq_cst);
    req->sent = mask;
    i = 0;
    do
    {
        SYS_PTR(
            &iobref, iobref_new, (),
            ENOMEM,
            E(),
            GOTO(failed_txn)
        );
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
        iobuf_unref(iobuf);

        idx = sys_bits_first_one_index64(mask);
        ida_rabin_split(size, ida->fragments, idx, buffer, iobuf->ptr);

        vector[0].iov_base = iobuf->ptr;
        vector[0].iov_len = size / ida->fragments;

        SYS_IO(sys_gf_writev_wind, (req->frame, NULL, ida->xl_list[idx],
                                    args->fd, vector, 1,
                                    offset / ida->fragments, args->flags,
                                    iobref, args->xdata),
               SYS_CBK(ida_dispatch_write_cbk, (ida, req, idx)));

        iobref_unref(iobref);

        mask ^= 1ULL << idx;
    } while (++i < count);

    dfc_end(req->txn, count);

    SYS_FREE_ALIGNED(buffer);

    return;

failed_iobuf:
    iobuf_unref(iobuf);
failed_iobref:
    iobref_unref(iobref);
failed_txn:
    dfc_end(req->txn, 0);
failed:
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
                                                  (size_t, tail)))
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
        memcpy(ptr, args->vector.iov_base, ida->block_size);
    }

    __ida_dispatch_write(ida, req, buffer, offset, size, head, tail);
}

void ida_dispatch_write(ida_private_t * ida, ida_request_t * req)
{
    SYS_GF_FOP_CALL_TYPE(writev) * args;
    uint8_t * buffer;
    off_t user_offs, offs;
    size_t user_size, size, head, tail, tmp;

    SYS_TEST(
        req->sent == 0,
        EIO,
        D(),
        GOTO(failed)
    );

    args = (SYS_GF_FOP_CALL_TYPE(writev) *)((uintptr_t *)req + IDA_REQ_SIZE);

    user_offs = args->offset;
    user_size = iov_length(&args->vector, args->count);

    head = user_offs % ida->block_size;
    offs = user_offs - head;
    size = user_size + head;
    tmp = ida->block_size - 1;
    tail = tmp - (size + tmp) % ida->block_size;
    size += tail;

    req->data = 1 + (head > 0) + ((tail > 0) && (size > ida->block_size));
    req->flags = 0;
    SYS_ALLOC_ALIGNED(
        &buffer, size, 16, sys_mt_uint8_t,
        E(),
        GOTO(failed)
    );

    if (head > 0)
    {
        SYS_IO(sys_gf_readv_wind, (req->frame, NULL, ida->xl, args->fd,
                                   ida->block_size, offs, 0, NULL),
               SYS_CBK(ida_dispatch_write_readv_cbk, (ida, req, buffer,
                                                      buffer, offs, size,
                                                      head, tail)
                      ));
    }

    if ((tail > 0) && (size > ida->block_size))
    {
        SYS_IO(sys_gf_readv_wind, (req->frame, NULL, ida->xl, args->fd,
                                   ida->block_size,
                                   offs + size - ida->block_size, 0, NULL),
               SYS_CBK(ida_dispatch_write_readv_cbk, (ida, req, buffer,
                                                      buffer + size -
                                                      ida->block_size, offs,
                                                      size, head, tail)
                      ));
    }

    __ida_dispatch_write(ida, req, buffer, offs, size, head, tail);

    return;

failed:
    logE("WRITE failed in ida_dispatch_write");
    ida_unwind(req, EIO, (uintptr_t *)req + IDA_REQ_SIZE);
}
