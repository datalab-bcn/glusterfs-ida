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
#include "gfdfc.h"

#include "ida.h"
#include "ida-common.h"
#include "ida-type-loc.h"
#include "ida-type-dict.h"
#include "ida-type-inode.h"
#include "ida-type-fd.h"
#include "ida-type-iatt.h"
#include "ida-type-lock.h"
#include "ida-type-statvfs.h"
#include "ida-manager.h"
#include "ida-rabin.h"

bool ida_prepare_access(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_access(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                        uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(access) * dst;
    SYS_GF_WIND_CBK_TYPE(access) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(access) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(access) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_access(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_create(ida_private_t * ida, ida_request_t * req)
{
    SYS_GF_FOP_CALL_TYPE(create) * args;

    args = (SYS_GF_FOP_CALL_TYPE(create) *)((uintptr_t *)req + IDA_REQ_SIZE);

    SYS_CALL(
        sys_dict_set_uint64, (&args->xdata, DFC_XATTR_OFFSET, 0, NULL),
        E(),
        RETVAL(false)
    );

    if ((args->flags & O_ACCMODE) == O_WRONLY)
    {
        args->flags = (args->flags & ~O_ACCMODE) | O_RDWR;
    }

    return true;
}

bool ida_combine_create(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                        uintptr_t * data)
{
    struct iatt buf, preparent, postparent;
    SYS_GF_CBK_CALL_TYPE(create) * dst;
    SYS_GF_WIND_CBK_TYPE(create) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(create) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(create) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if ((src->fd != dst->fd) || (src->inode != dst->inode) ||
            !ida_iatt_combine(&buf, &dst->buf, &src->buf) ||
            !ida_iatt_combine(&preparent, &dst->preparent, &src->preparent) ||
            !ida_iatt_combine(&postparent, &dst->postparent, &src->postparent))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
        memcpy(&dst->preparent, &preparent, sizeof(dst->preparent));
        memcpy(&dst->postparent, &postparent, sizeof(dst->postparent));
    }

    return true;
}

int32_t ida_rebuild_create(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(create) * args;

    args = (SYS_GF_CBK_CALL_TYPE(create) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
        ida_iatt_rebuild(ida, &args->preparent);
        ida_iatt_rebuild(ida, &args->postparent);
    }

    return 0;
}

bool ida_prepare_flush(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_flush(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                       uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(flush) * dst;
    SYS_GF_WIND_CBK_TYPE(flush) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(flush) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(flush) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_flush(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_fsync(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_fsync(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                       uintptr_t * data)
{
    struct iatt prebuf, postbuf;
    SYS_GF_CBK_CALL_TYPE(fsync) * dst;
    SYS_GF_WIND_CBK_TYPE(fsync) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(fsync) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(fsync) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&prebuf, &dst->prebuf, &src->prebuf) ||
            !ida_iatt_combine(&postbuf, &dst->postbuf, &src->postbuf))
        {
            return false;
        }

        memcpy(&dst->prebuf, &prebuf, sizeof(dst->prebuf));
        memcpy(&dst->postbuf, &postbuf, sizeof(dst->postbuf));
    }

    return true;
}

int32_t ida_rebuild_fsync(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(fsync) * args;

    args = (SYS_GF_CBK_CALL_TYPE(fsync) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->prebuf);
        ida_iatt_rebuild(ida, &args->postbuf);
    }

    return 0;
}

bool ida_prepare_fsyncdir(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_fsyncdir(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(fsyncdir) * dst;
    SYS_GF_WIND_CBK_TYPE(fsyncdir) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(fsyncdir) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(fsyncdir) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_fsyncdir(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_link(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_link(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                      uintptr_t * data)
{
    struct iatt buf, preparent, postparent;
    SYS_GF_CBK_CALL_TYPE(link) * dst;
    SYS_GF_WIND_CBK_TYPE(link) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(link) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(link) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if ((src->inode != dst->inode) ||
            !ida_iatt_combine(&buf, &dst->buf, &src->buf) ||
            !ida_iatt_combine(&preparent, &dst->preparent, &src->preparent) ||
            !ida_iatt_combine(&postparent, &dst->postparent, &src->postparent))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
        memcpy(&dst->preparent, &preparent, sizeof(dst->preparent));
        memcpy(&dst->postparent, &postparent, sizeof(dst->postparent));
    }

    return true;
}

int32_t ida_rebuild_link(ida_private_t * ida, ida_request_t * req,
                         ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(link) * args;

    args = (SYS_GF_CBK_CALL_TYPE(link) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
        ida_iatt_rebuild(ida, &args->preparent);
        ida_iatt_rebuild(ida, &args->postparent);
    }

    return 0;
}

bool ida_prepare_lk(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_lk(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                    uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(lk) * dst;
    SYS_GF_WIND_CBK_TYPE(lk) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(lk) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(lk) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        return ida_lock_compare(&dst->flock, &src->flock);
    }

    return true;
}

int32_t ida_rebuild_lk(ida_private_t * ida, ida_request_t * req,
                       ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_inodelk(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_inodelk(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                         uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(inodelk) * dst;
    SYS_GF_WIND_CBK_TYPE(inodelk) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(inodelk) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(inodelk) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_inodelk(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_finodelk(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_finodelk(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(finodelk) * dst;
    SYS_GF_WIND_CBK_TYPE(finodelk) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(finodelk) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(finodelk) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_finodelk(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_entrylk(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_entrylk(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                         uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(entrylk) * dst;
    SYS_GF_WIND_CBK_TYPE(entrylk) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(entrylk) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(entrylk) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_entrylk(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_fentrylk(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_fentrylk(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(fentrylk) * dst;
    SYS_GF_WIND_CBK_TYPE(fentrylk) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(fentrylk) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(fentrylk) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_fentrylk(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_lookup(ida_private_t * ida, ida_request_t * req)
{
    SYS_GF_FOP_CALL_TYPE(lookup) * args;
    data_t * data;
    size_t size, tmp;

    args = (SYS_GF_FOP_CALL_TYPE(lookup) *)((uintptr_t *)req + IDA_REQ_SIZE);
    if (sys_dict_del(&args->xdata, GF_CONTENT_KEY, &data) == 0)
    {
        size = data_to_uint64(data);
        data_unref(data);

        req->size = size;
        tmp = ida->block_size - 1;
        size += tmp - (size + tmp) % ida->block_size;
        req->data = size;
        size /= ida->fragments;

        SYS_PTR(
            &data, data_from_uint64, (size),
            ENOMEM,
            E(),
            RETVAL(true)
        );

        SYS_CALL(
            sys_dict_set, (&args->xdata, GF_CONTENT_KEY, data, NULL),
            E(),
            GOTO(failed)
        );
    }

    return true;

failed:
    data_unref(data);

    return true;
}

bool ida_combine_lookup(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                        uintptr_t * data)
{
    struct iatt buf, postparent;
    ida_answer_t * next;
    SYS_GF_CBK_CALL_TYPE(lookup) * dst;
    SYS_GF_WIND_CBK_TYPE(lookup) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(lookup) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(lookup) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        logW("Answers differ");
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if ((src->inode != dst->inode) ||
            !ida_iatt_combine(&buf, &dst->buf, &src->buf) ||
            !ida_iatt_combine(&postparent, &dst->postparent, &src->postparent))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
        memcpy(&dst->postparent, &postparent, sizeof(dst->postparent));

        next = req->handlers->copy(data);
        next->count = 0;
        next->id = idx;
        next->next = ans->next;

        ans->next = next;
    }

    return true;
}

int32_t ida_rebuild_lookup(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(lookup) * args, * tmp;
    ida_answer_t * item;
    uint8_t * blocks[ans->count];
    uint32_t values[ans->count];
    uint8_t * buff;
    data_t * data;
    size_t size;
    int32_t i;

    args = (SYS_GF_CBK_CALL_TYPE(lookup) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
        ida_iatt_rebuild(ida, &args->postparent);

        size = SIZE_MAX;
        for (i = 0, item = ans;
             (item != NULL) && (i < ida->fragments);
             item = item->next)
        {
            tmp = (SYS_GF_CBK_CALL_TYPE(lookup) *)((uintptr_t *)item +
                                                   IDA_ANS_SIZE);
            if (sys_dict_get(tmp->xdata, GF_CONTENT_KEY, &data) == 0)
            {
                values[i] = item->id;
                blocks[i] = (uint8_t *)data->data;

                if (size > data->len)
                {
                    size = data->len;
                }

                i++;
            }
        }

        sys_dict_del(&args->xdata, GF_CONTENT_KEY, NULL);

        if (i >= ida->fragments)
        {
            size -= size % (ida->block_size / ida->fragments);
            if (size > 0)
            {
                SYS_ALLOC(
                    &buff, req->data, sys_mt_uint8_t,
                    E(),
                    GOTO(done)
                );

                ida_rabin_merge(size, ida->fragments, values, blocks, buff);

                size *= ida->fragments;
                if (size > req->size)
                {
                    size = req->size;
                }

                SYS_PTR(
                    &data, data_from_dynptr, (buff, size),
                    ENOMEM,
                    E(),
                    GOTO(failed_buff)
                );

                SYS_CALL(
                    sys_dict_set, (&args->xdata, GF_CONTENT_KEY, data, NULL),
                    E(),
                    GOTO(failed_data)
                );
            }
        }
    }

done:
    return 0;

failed_buff:
    SYS_FREE(buff);

    return 0;

failed_data:
    data_unref(data);

    return 0;
}

bool ida_prepare_mkdir(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_mkdir(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                       uintptr_t * data)
{
    struct iatt buf, preparent, postparent;
    SYS_GF_CBK_CALL_TYPE(mkdir) * dst;
    SYS_GF_WIND_CBK_TYPE(mkdir) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(mkdir) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(mkdir) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if ((src->inode != dst->inode) ||
            !ida_iatt_combine(&buf, &dst->buf, &src->buf) ||
            !ida_iatt_combine(&preparent, &dst->preparent, &src->preparent) ||
            !ida_iatt_combine(&postparent, &dst->postparent, &src->postparent))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
        memcpy(&dst->preparent, &preparent, sizeof(dst->preparent));
        memcpy(&dst->postparent, &postparent, sizeof(dst->postparent));
    }

    return true;
}

int32_t ida_rebuild_mkdir(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(mkdir) * args;

    args = (SYS_GF_CBK_CALL_TYPE(mkdir) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
        ida_iatt_rebuild(ida, &args->preparent);
        ida_iatt_rebuild(ida, &args->postparent);
    }

    return 0;
}

bool ida_prepare_mknod(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_mknod(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                       uintptr_t * data)
{
    struct iatt buf, preparent, postparent;
    SYS_GF_CBK_CALL_TYPE(mknod) * dst;
    SYS_GF_WIND_CBK_TYPE(mknod) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(mknod) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(mknod) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if ((src->inode != dst->inode) ||
            !ida_iatt_combine(&buf, &dst->buf, &src->buf) ||
            !ida_iatt_combine(&preparent, &dst->preparent, &src->preparent) ||
            !ida_iatt_combine(&postparent, &dst->postparent, &src->postparent))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
        memcpy(&dst->preparent, &preparent, sizeof(dst->preparent));
        memcpy(&dst->postparent, &postparent, sizeof(dst->postparent));
    }

    return true;
}

int32_t ida_rebuild_mknod(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(mknod) * args;

    args = (SYS_GF_CBK_CALL_TYPE(mknod) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
        ida_iatt_rebuild(ida, &args->preparent);
        ida_iatt_rebuild(ida, &args->postparent);
    }

    return 0;
}

bool ida_prepare_open(ida_private_t * ida, ida_request_t * req)
{
    SYS_GF_FOP_CALL_TYPE(open) * args;

    args = (SYS_GF_FOP_CALL_TYPE(open) *)((uintptr_t *)req + IDA_REQ_SIZE);

    if ((args->flags & O_ACCMODE) == O_WRONLY)
    {
        args->flags = (args->flags & ~O_ACCMODE) | O_RDWR;
    }

    return true;
}

bool ida_combine_open(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                      uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(open) * dst;
    SYS_GF_WIND_CBK_TYPE(open) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(open) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(open) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (src->fd != dst->fd)
        {
            return false;
        }
    }

    return true;
}

int32_t ida_rebuild_open(ida_private_t * ida, ida_request_t * req,
                         ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_opendir(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_opendir(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                         uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(opendir) * dst;
    SYS_GF_WIND_CBK_TYPE(opendir) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(opendir) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(opendir) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (src->fd != dst->fd)
        {
            return false;
        }
    }

    return true;
}

int32_t ida_rebuild_opendir(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_rchecksum(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_rchecksum(ida_request_t * req, uint32_t idx,
                           ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(rchecksum) * dst;
    SYS_GF_WIND_CBK_TYPE(rchecksum) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(rchecksum) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(rchecksum) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_rchecksum(ida_private_t * ida, ida_request_t * req,
                              ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_readdir(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_readdir(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                         uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(readdir) * dst;
    SYS_GF_WIND_CBK_TYPE(readdir) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(readdir) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(readdir) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_readdir(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(readdir) * args;
    gf_dirent_t * entry;

    args = (SYS_GF_CBK_CALL_TYPE(readdir) *)((uintptr_t *)ans + IDA_ANS_SIZE);

    if (args->op_ret >= 0)
    {
        list_for_each_entry(entry, &args->entries.list, list)
        {
            ida_iatt_rebuild(ida, &entry->d_stat);
        }
    }

    return 0;
}

bool ida_prepare_readdirp(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_readdirp(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(readdirp) * dst;
    SYS_GF_WIND_CBK_TYPE(readdirp) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(readdirp) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(readdirp) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_readdirp(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(readdirp) * args;
    gf_dirent_t * entry;

    args = (SYS_GF_CBK_CALL_TYPE(readdirp) *)((uintptr_t *)ans + IDA_ANS_SIZE);

    if (args->op_ret >= 0)
    {
        list_for_each_entry(entry, &args->entries.list, list)
        {
            ida_iatt_rebuild(ida, &entry->d_stat);
        }
    }

    return 0;
}

bool ida_prepare_readlink(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_readlink(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    struct iatt buf;
    SYS_GF_CBK_CALL_TYPE(readlink) * dst;
    SYS_GF_WIND_CBK_TYPE(readlink) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(readlink) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(readlink) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if ((strcmp(dst->path, src->path) != 0) ||
            !ida_iatt_combine(&buf, &dst->buf, &src->buf))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
    }

    return true;
}

int32_t ida_rebuild_readlink(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(readlink) * args;

    args = (SYS_GF_CBK_CALL_TYPE(readlink) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
    }

    return 0;
}


bool ida_prepare_readv(ida_private_t * ida, ida_request_t * req)
{
    SYS_GF_FOP_CALL_TYPE(readv) * args;
    off_t offs;
    size_t size, head, tail, tmp;

    args = (SYS_GF_FOP_CALL_TYPE(readv) *)((uintptr_t *)req + IDA_REQ_SIZE);

    head = args->offset % ida->block_size;
    offs = args->offset - head;
    size = args->size + head;
    tmp = ida->block_size - 1;
    tail = tmp - (size + tmp) % ida->block_size;
    size += tail;

    req->data = head;
    req->size = args->size;

    args->offset = offs / ida->fragments;
    args->size = size / ida->fragments;

    return true;
}

bool ida_combine_readv(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                       uintptr_t * data)
{
    struct iatt stbuf;
    ida_answer_t * next;
    SYS_GF_CBK_CALL_TYPE(readv) * dst;
    SYS_GF_WIND_CBK_TYPE(readv) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(readv) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(readv) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&stbuf, &dst->stbuf, &src->stbuf))
        {
            return false;
        }

        memcpy(&dst->stbuf, &stbuf, sizeof(dst->stbuf));

        next = req->handlers->copy(data);
        next->count = 0;
        next->id = idx;
        next->next = ans->next;

        ans->next = next;
    }

    return true;
}

int32_t ida_rebuild_readv(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(readv) * args, * tmp;
    ida_answer_t * item;
    uint8_t * ptr;
    uint8_t * blocks[ans->count];
    uint8_t * ptrs[ans->count];
    uint32_t values[ans->count];
    struct iobref * iobref;
    struct iobuf * iobuf;
    size_t size, min, max, slice;
    int32_t i, j;

    memset(blocks, 0, sizeof(blocks));

    args = (SYS_GF_CBK_CALL_TYPE(readv) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        struct iovec vector[args->vector.count * ida->fragments];

        ida_iatt_rebuild(ida, &args->stbuf);

        min = SIZE_MAX;
        for (i = 0, item = ans; item != NULL; i++, item = item->next)
        {
            tmp = (SYS_GF_CBK_CALL_TYPE(readv) *)((uintptr_t *)item +
                                                  IDA_ANS_SIZE);
            values[i] = item->id;
            size = iov_length(tmp->vector.iovec, tmp->vector.count);
            if (min > size)
            {
                min = size;
            }
            SYS_ALLOC_ALIGNED(
                &ptr, size, 16, sys_mt_uint8_t,
                E(),
                GOTO(failed)
            );
            ptrs[i] = blocks[i] = ptr;
            for (j = 0; j < tmp->vector.count; j++)
            {
                memcpy(ptr, tmp->vector.iovec[j].iov_base,
                       tmp->vector.iovec[j].iov_len);
                ptr += tmp->vector.iovec[j].iov_len;
            }
        }
        size = min % (ida->block_size / ida->fragments);
        min -= size;

        SYS_PTR(
            &iobref, iobref_new, (),
            ENOMEM,
            E(),
            GOTO(failed)
        );
        size = min;
        max = iobpool_default_pagesize(
                                (struct iobuf_pool *)ida->xl->ctx->iobuf_pool);
        max /= ida->fragments;
        j = 0;
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

            slice = size;
            if (slice > max)
            {
                slice = max;
            }
            ida_rabin_merge(slice, ida->fragments, values, ptrs, iobuf->ptr);

            size -= slice;
            for (i = 0; i < ans->count; i++)
            {
                ptrs[i] += slice;
            }
            vector[j].iov_base = iobuf->ptr;
            vector[j].iov_len = slice;
            j++;

            iobuf_unref(iobuf);
        } while (size > 0);

        vector[0].iov_base += req->data;
        size = min * ida->fragments - req->data;
        while (size > req->size)
        {
            if (size - req->size >= vector[j - 1].iov_len)
            {
                size -= vector[--j].iov_len;
            }
            else
            {
                vector[j - 1].iov_len -= size - req->size;
                size = req->size;
            }
        }

        iobref_unref(args->iobref);
        args->iobref = iobref;
        sys_iovec_acquire(&args->vector, vector, j);

        args->op_ret = size;
    }

    return 0;

failed_iobuf:
    iobuf_unref(iobuf);
failed_iobref:
    iobref_unref(iobref);
failed:
    for (i = 0; i < ans->count; i++)
    {
        if (blocks[i] != NULL)
        {
            SYS_FREE_ALIGNED(blocks[i]);
        }
    }
    return -1;
}

bool ida_prepare_rename(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_rename(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                        uintptr_t * data)
{
    struct iatt buf, preoldparent, postoldparent, prenewparent, postnewparent;
    SYS_GF_CBK_CALL_TYPE(rename) * dst;
    SYS_GF_WIND_CBK_TYPE(rename) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(rename) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(rename) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&buf, &dst->buf, &src->buf) ||
            !ida_iatt_combine(&preoldparent, &dst->preoldparent,
                              &src->preoldparent) ||
            !ida_iatt_combine(&postoldparent, &dst->postoldparent,
                              &src->postoldparent) ||
            !ida_iatt_combine(&prenewparent, &dst->prenewparent,
                              &src->prenewparent) ||
            !ida_iatt_combine(&postnewparent, &dst->postnewparent,
                              &src->postnewparent))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
        memcpy(&dst->preoldparent, &preoldparent, sizeof(dst->preoldparent));
        memcpy(&dst->postoldparent, &postoldparent,
               sizeof(dst->postoldparent));
        memcpy(&dst->prenewparent, &prenewparent, sizeof(dst->prenewparent));
        memcpy(&dst->postnewparent, &postnewparent,
               sizeof(dst->postnewparent));
    }

    return true;
}

int32_t ida_rebuild_rename(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(rename) * args;

    args = (SYS_GF_CBK_CALL_TYPE(rename) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
        ida_iatt_rebuild(ida, &args->preoldparent);
        ida_iatt_rebuild(ida, &args->postoldparent);
        ida_iatt_rebuild(ida, &args->prenewparent);
        ida_iatt_rebuild(ida, &args->postnewparent);
    }

    return 0;
}

bool ida_prepare_rmdir(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_rmdir(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                       uintptr_t * data)
{
    struct iatt preparent, postparent;
    SYS_GF_CBK_CALL_TYPE(rmdir) * dst;
    SYS_GF_WIND_CBK_TYPE(rmdir) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(rmdir) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(rmdir) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&preparent, &dst->preparent, &src->preparent) ||
            !ida_iatt_combine(&postparent, &dst->postparent, &src->postparent))
        {
            return false;
        }

        memcpy(&dst->preparent, &preparent, sizeof(dst->preparent));
        memcpy(&dst->postparent, &postparent, sizeof(dst->postparent));
    }

    return true;
}

int32_t ida_rebuild_rmdir(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(rmdir) * args;

    args = (SYS_GF_CBK_CALL_TYPE(rmdir) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->preparent);
        ida_iatt_rebuild(ida, &args->postparent);
    }

    return 0;
}

bool ida_prepare_stat(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_stat(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                      uintptr_t * data)
{
    struct iatt buf;
    SYS_GF_CBK_CALL_TYPE(stat) * dst;
    SYS_GF_WIND_CBK_TYPE(stat) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(stat) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(stat) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&buf, &dst->buf, &src->buf))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
    }

    return true;
}

int32_t ida_rebuild_stat(ida_private_t * ida, ida_request_t * req,
                         ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(stat) * args;

    args = (SYS_GF_CBK_CALL_TYPE(stat) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
    }

    return 0;
}

bool ida_prepare_fstat(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_fstat(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                       uintptr_t * data)
{
    struct iatt buf;
    SYS_GF_CBK_CALL_TYPE(fstat) * dst;
    SYS_GF_WIND_CBK_TYPE(fstat) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(fstat) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(fstat) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&buf, &dst->buf, &src->buf))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
    }

    return true;
}

int32_t ida_rebuild_fstat(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(fstat) * args;

    args = (SYS_GF_CBK_CALL_TYPE(fstat) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
    }

    return 0;
}

bool ida_prepare_setattr(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_setattr(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                         uintptr_t * data)
{
    struct iatt preop_stbuf, postop_stbuf;
    SYS_GF_CBK_CALL_TYPE(setattr) * dst;
    SYS_GF_WIND_CBK_TYPE(setattr) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(setattr) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(setattr) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&preop_stbuf, &dst->preop_stbuf,
                              &src->preop_stbuf) ||
            !ida_iatt_combine(&postop_stbuf, &dst->postop_stbuf,
                              &src->postop_stbuf))
        {
            return false;
        }

        memcpy(&dst->preop_stbuf, &preop_stbuf, sizeof(dst->preop_stbuf));
        memcpy(&dst->postop_stbuf, &postop_stbuf, sizeof(dst->postop_stbuf));
    }

    return true;
}

int32_t ida_rebuild_setattr(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(setattr) * args;

    args = (SYS_GF_CBK_CALL_TYPE(setattr) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->preop_stbuf);
        ida_iatt_rebuild(ida, &args->postop_stbuf);
    }

    return 0;
}

bool ida_prepare_fsetattr(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_fsetattr(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    struct iatt preop_stbuf, postop_stbuf;
    SYS_GF_CBK_CALL_TYPE(fsetattr) * dst;
    SYS_GF_WIND_CBK_TYPE(fsetattr) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(fsetattr) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(fsetattr) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&preop_stbuf, &dst->preop_stbuf,
                              &src->preop_stbuf) ||
            !ida_iatt_combine(&postop_stbuf, &dst->postop_stbuf,
                              &src->postop_stbuf))
        {
            return false;
        }

        memcpy(&dst->preop_stbuf, &preop_stbuf, sizeof(dst->preop_stbuf));
        memcpy(&dst->postop_stbuf, &postop_stbuf, sizeof(dst->postop_stbuf));
    }

    return true;
}

int32_t ida_rebuild_fsetattr(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(fsetattr) * args;

    args = (SYS_GF_CBK_CALL_TYPE(fsetattr) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->preop_stbuf);
        ida_iatt_rebuild(ida, &args->postop_stbuf);
    }

    return 0;
}

bool ida_prepare_statfs(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_statfs(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                        uintptr_t * data)
{
    struct statvfs buf;
    SYS_GF_CBK_CALL_TYPE(statfs) * dst;
    SYS_GF_WIND_CBK_TYPE(statfs) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(statfs) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(statfs) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_statvfs_combine(&buf, &dst->buf, &src->buf))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
    }

    return true;
}

int32_t ida_rebuild_statfs(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(statfs) * args;

    args = (SYS_GF_CBK_CALL_TYPE(statfs) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        args->buf.f_blocks *= ida->fragments;
        args->buf.f_bfree *= ida->fragments;
        args->buf.f_bavail *= ida->fragments;
    }

    return 0;
}

bool ida_prepare_symlink(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_symlink(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                         uintptr_t * data)
{
    struct iatt buf, preparent, postparent;
    SYS_GF_CBK_CALL_TYPE(symlink) * dst;
    SYS_GF_WIND_CBK_TYPE(symlink) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(symlink) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(symlink) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if ((src->inode != dst->inode) ||
            !ida_iatt_combine(&buf, &dst->buf, &src->buf) ||
            !ida_iatt_combine(&preparent, &dst->preparent, &src->preparent) ||
            !ida_iatt_combine(&postparent, &dst->postparent, &src->postparent))
        {
            return false;
        }

        memcpy(&dst->buf, &buf, sizeof(dst->buf));
        memcpy(&dst->preparent, &preparent, sizeof(dst->preparent));
        memcpy(&dst->postparent, &postparent, sizeof(dst->postparent));
    }

    return true;
}

int32_t ida_rebuild_symlink(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(symlink) * args;

    args = (SYS_GF_CBK_CALL_TYPE(symlink) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->buf);
        ida_iatt_rebuild(ida, &args->preparent);
        ida_iatt_rebuild(ida, &args->postparent);
    }

    return 0;
}

bool ida_prepare_truncate(ida_private_t * ida, ida_request_t * req)
{
    SYS_GF_FOP_CALL_TYPE(truncate) * args;
    size_t tmp;

    args = (SYS_GF_FOP_CALL_TYPE(truncate) *)((uintptr_t *)req + IDA_REQ_SIZE);

    SYS_CALL(
        sys_dict_set_uint64, (&args->xdata, DFC_XATTR_OFFSET, args->offset,
                              NULL),
        E(),
        RETVAL(false)
    );

    tmp = ida->block_size - 1;
    args->offset += tmp - (args->offset + tmp) % ida->block_size;
    args->offset /= ida->fragments;

    return true;
}

bool ida_combine_truncate(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    struct iatt prebuf, postbuf;
    SYS_GF_CBK_CALL_TYPE(truncate) * dst;
    SYS_GF_WIND_CBK_TYPE(truncate) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(truncate) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(truncate) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&prebuf, &dst->prebuf, &src->prebuf) ||
            !ida_iatt_combine(&postbuf, &dst->postbuf, &src->postbuf))
        {
            return false;
        }

        memcpy(&dst->prebuf, &prebuf, sizeof(dst->prebuf));
        memcpy(&dst->postbuf, &postbuf, sizeof(dst->postbuf));
    }

    return true;
}

int32_t ida_rebuild_truncate(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(truncate) * args;

    args = (SYS_GF_CBK_CALL_TYPE(truncate) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->prebuf);
        ida_iatt_rebuild(ida, &args->postbuf);
    }

    return 0;
}

bool ida_prepare_ftruncate(ida_private_t * ida, ida_request_t * req)
{
    SYS_GF_FOP_CALL_TYPE(ftruncate) * args;
    size_t tmp;

    args = (SYS_GF_FOP_CALL_TYPE(ftruncate) *)((uintptr_t *)req +
                                               IDA_REQ_SIZE);

    SYS_CALL(
        sys_dict_set_uint64, (&args->xdata, DFC_XATTR_OFFSET, args->offset,
                              NULL),
        E(),
        RETVAL(false)
    );

    tmp = ida->block_size - 1;
    args->offset += tmp - (args->offset + tmp) % ida->block_size;
    args->offset /= ida->fragments;

    return true;
}

bool ida_combine_ftruncate(ida_request_t * req, uint32_t idx,
                           ida_answer_t * ans, uintptr_t * data)
{
    struct iatt prebuf, postbuf;
    SYS_GF_CBK_CALL_TYPE(ftruncate) * dst;
    SYS_GF_WIND_CBK_TYPE(ftruncate) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(ftruncate) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(ftruncate) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&prebuf, &dst->prebuf, &src->prebuf) ||
            !ida_iatt_combine(&postbuf, &dst->postbuf, &src->postbuf))
        {
            return false;
        }

        memcpy(&dst->prebuf, &prebuf, sizeof(dst->prebuf));
        memcpy(&dst->postbuf, &postbuf, sizeof(dst->postbuf));
    }

    return true;
}

int32_t ida_rebuild_ftruncate(ida_private_t * ida, ida_request_t * req,
                              ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(ftruncate) * args;

    args = (SYS_GF_CBK_CALL_TYPE(ftruncate) *)((uintptr_t *)ans +
                                               IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->prebuf);
        ida_iatt_rebuild(ida, &args->postbuf);
    }

    return 0;
}

bool ida_prepare_unlink(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_unlink(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                        uintptr_t * data)
{
    struct iatt preparent, postparent;
    SYS_GF_CBK_CALL_TYPE(unlink) * dst;
    SYS_GF_WIND_CBK_TYPE(unlink) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(unlink) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(unlink) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&preparent, &dst->preparent, &src->preparent) ||
            !ida_iatt_combine(&postparent, &dst->postparent, &src->postparent))
        {
            return false;
        }

        memcpy(&dst->preparent, &preparent, sizeof(dst->preparent));
        memcpy(&dst->postparent, &postparent, sizeof(dst->postparent));
    }

    return true;
}

int32_t ida_rebuild_unlink(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(unlink) * args;

    args = (SYS_GF_CBK_CALL_TYPE(unlink) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->preparent);
        ida_iatt_rebuild(ida, &args->postparent);
    }

    return 0;
}

bool ida_prepare_writev(ida_private_t * ida, ida_request_t * req)
{
    SYS_GF_FOP_CALL_TYPE(writev) * args;

    args = (SYS_GF_FOP_CALL_TYPE(writev) *)((uintptr_t *)req + IDA_REQ_SIZE);

    SYS_CALL(
        sys_dict_set_uint64, (&args->xdata, DFC_XATTR_OFFSET, args->offset,
                              NULL),
        E(),
        RETVAL(false)
    );

    SYS_CALL(
        sys_dict_set_uint64, (&args->xdata, DFC_XATTR_SIZE,
                              iov_length(args->vector.iovec,
                                         args->vector.count),
                              NULL),
        E(),
        RETVAL(false)
    );

    return true;
}

bool ida_combine_writev(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                        uintptr_t * data)
{
    struct iatt prebuf, postbuf;
    SYS_GF_CBK_CALL_TYPE(writev) * dst;
    SYS_GF_WIND_CBK_TYPE(writev) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(writev) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(writev) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        if (!ida_iatt_combine(&prebuf, &dst->prebuf, &src->prebuf) ||
            !ida_iatt_combine(&postbuf, &dst->postbuf, &src->postbuf))
        {
            return false;
        }

        memcpy(&dst->prebuf, &prebuf, sizeof(dst->prebuf));
        memcpy(&dst->postbuf, &postbuf, sizeof(dst->postbuf));
    }

    return true;
}

int32_t ida_rebuild_writev(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * ans)
{
    SYS_GF_CBK_CALL_TYPE(writev) * args;

    args = (SYS_GF_CBK_CALL_TYPE(writev) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    if (args->op_ret >= 0)
    {
        ida_iatt_rebuild(ida, &args->prebuf);
        ida_iatt_rebuild(ida, &args->postbuf);

        args->op_ret *= ida->fragments;
        args->op_ret -= req->size;
    }

    return 0;
}

bool ida_prepare_getxattr(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_getxattr(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(getxattr) * dst;
    SYS_GF_WIND_CBK_TYPE(getxattr) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(getxattr) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(getxattr) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        return ida_dict_compare(dst->dict, src->dict);
    }

    return true;
}

int32_t ida_rebuild_getxattr(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_fgetxattr(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_fgetxattr(ida_request_t * req, uint32_t idx,
                           ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(fgetxattr) * dst;
    SYS_GF_WIND_CBK_TYPE(fgetxattr) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(fgetxattr) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(fgetxattr) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        return ida_dict_compare(dst->dict, src->dict);
    }

    return true;
}

int32_t ida_rebuild_fgetxattr(ida_private_t * ida, ida_request_t * req,
                              ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_setxattr(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_setxattr(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(setxattr) * dst;
    SYS_GF_WIND_CBK_TYPE(setxattr) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(setxattr) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(setxattr) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_setxattr(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_fsetxattr(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_fsetxattr(ida_request_t * req, uint32_t idx,
                           ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(fsetxattr) * dst;
    SYS_GF_WIND_CBK_TYPE(fsetxattr) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(fsetxattr) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(fsetxattr) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_fsetxattr(ida_private_t * ida, ida_request_t * req,
                              ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_removexattr(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_removexattr(ida_request_t * req, uint32_t idx,
                             ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(removexattr) * dst;
    SYS_GF_WIND_CBK_TYPE(removexattr) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(removexattr) *)((uintptr_t *)ans +
                                                IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(removexattr) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_removexattr(ida_private_t * ida, ida_request_t * req,
                                ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_fremovexattr(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_fremovexattr(ida_request_t * req, uint32_t idx,
                              ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(fremovexattr) * dst;
    SYS_GF_WIND_CBK_TYPE(fremovexattr) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(fremovexattr) *)((uintptr_t *)ans +
                                                 IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(fremovexattr) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    return true;
}

int32_t ida_rebuild_fremovexattr(ida_private_t * ida, ida_request_t * req,
                                 ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_xattrop(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_xattrop(ida_request_t * req, uint32_t idx, ida_answer_t * ans,
                         uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(xattrop) * dst;
    SYS_GF_WIND_CBK_TYPE(xattrop) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(xattrop) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(xattrop) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        return ida_dict_compare(dst->xattr, src->xattr);
    }

    return true;
}

int32_t ida_rebuild_xattrop(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * ans)
{
    return 0;
}

bool ida_prepare_fxattrop(ida_private_t * ida, ida_request_t * req)
{
    return true;
}

bool ida_combine_fxattrop(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data)
{
    SYS_GF_CBK_CALL_TYPE(fxattrop) * dst;
    SYS_GF_WIND_CBK_TYPE(fxattrop) * src;

    dst = (SYS_GF_CBK_CALL_TYPE(fxattrop) *)((uintptr_t *)ans + IDA_ANS_SIZE);
    src = (SYS_GF_WIND_CBK_TYPE(fxattrop) *)data;

    if ((dst->op_ret != src->op_ret) || (dst->op_errno != src->op_errno) ||
        !ida_dict_compare(dst->xdata, src->xdata))
    {
        return false;
    }

    if (dst->op_ret >= 0)
    {
        return ida_dict_compare(dst->xattr, src->xattr);
    }

    return true;
}

int32_t ida_rebuild_fxattrop(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * ans)
{
    return 0;
}
