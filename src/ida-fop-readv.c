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

#include "ida.h"
#include "ida-common.h"
#include "ida-type-loc.h"
#include "ida-type-dict.h"
#include "ida-type-inode.h"
#include "ida-type-fd.h"
#include "ida-type-iatt.h"
#include "ida-type-buffer.h"
#include "ida-check.h"
#include "ida-memory.h"
#include "ida-manager.h"
#include "ida-rabin.h"

static int32_t ida_dispatch_readv_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct iovec * vectors, int32_t count, struct iatt * attr, struct iobref * buffers, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = 0;

    if (result >= 0)
    {
        error = ida_buffer_assign(local, &args->readv.buffer, buffers, vectors, count);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->readv.attr, attr);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->readv.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_readv(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(readv, frame, child, index, ida_dispatch_readv_cbk, args->readv.fd, args->readv.size, args->readv.offset, args->readv.flags, args->readv.xdata);

    return 0;
}

static int32_t ida_combine_readv(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if (dst->result >= 0)
    {
        if (likely(error == 0))
        {
            error = ida_buffer_combine(local, &dst->readv.buffer, &src->readv.buffer);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->readv.attr, &src->readv.attr);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->readv.xdata, src->readv.xdata);
    }

    return error;
}

static int32_t ida_rebuild_readv(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    int32_t index, count;
    size_t size, extra;
    struct iobref * buffers;
    struct iobuf * buffer;
    uint8_t * blocks[16];
    uint32_t values[16];

    if (args->result < 0)
    {
        return 0;
    }

    buffers = iobref_new();
    if (unlikely(buffers == NULL))
    {
        goto failed;
    }
    buffer = iobuf_get(local->xl->ctx->iobuf_pool);
    if (unlikely(buffer == NULL))
    {
        goto failed_iobref;
    }
    if (iobref_add(buffers, buffer) != 0)
    {
        goto failed_buffer;
    }

    extra = args->readv.buffer.vectors->iov_len % (16 * IDA_RABIN_BITS);
    if (extra != 0)
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Read data length is not valid. Removing overflow data.");
    }
    size = args->readv.buffer.vectors->iov_len - extra;
    if (unlikely(iobuf_pagesize(buffer) < size * local->private->fragments))
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Buffer size too big");

        goto failed_buffer;
    }

    count = 0;
    ida_mask_for_each(index, mask)
    {
        blocks[count] = local->args_cbk[index].readv.buffer.vectors->iov_base;
        values[count] = index;
        count++;
    }

    ida_iatt_adjust(local, &args->readv.attr, NULL, local->args.readv.fd->inode);

    ida_rabin_merge(args->readv.buffer.vectors->iov_len, local->private->fragments, values, blocks, buffer->ptr);

    iobref_unref(args->readv.buffer.buffers);

    extra = local->args.readv.user_offset - local->args.readv.offset * local->private->fragments;
    args->readv.buffer.buffers = buffers;
    args->readv.buffer.vectors[0].iov_base = (char *)buffer->ptr + extra;
    args->readv.buffer.vectors[0].iov_len = size * local->private->fragments - extra;
    args->readv.buffer.count = 1;

    if (args->readv.buffer.vectors[0].iov_len > local->args.readv.user_size)
    {
        args->readv.buffer.vectors[0].iov_len = local->args.readv.user_size;
    }
    if (!local->healing)
    {
        if (local->args.readv.user_offset + args->readv.buffer.vectors[0].iov_len > args->readv.attr.ia_size)
        {
            if (local->args.readv.user_offset > args->readv.attr.ia_size)
            {
                args->readv.buffer.vectors[0].iov_len = 0;
            }
            else
            {
                args->readv.buffer.vectors[0].iov_len = args->readv.attr.ia_size - local->args.readv.user_offset;
            }
        }
    }

    args->result = args->readv.buffer.vectors[0].iov_len;

    iobuf_unref(buffer);

    return 0;

failed_buffer:
    iobuf_unref(buffer);
failed_iobref:
    iobref_unref(buffers);
failed:
    return -ENOMEM;
}

static void ida_finish_readv(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_readv(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_fd_unassign(&local->args.readv.fd);
    ida_dict_unassign(&local->args.readv.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_buffer_unassign(&args->readv.buffer);
        ida_iatt_unassign(&args->readv.attr);
        ida_dict_unassign(&args->readv.xdata);
    }
}

static ida_manager_t ida_manager_readv =
{
    .name     = "readv",
    .dispatch = ida_dispatch_readv,
    .combine  = ida_combine_readv,
    .rebuild  = ida_rebuild_readv,
    .finish   = ida_finish_readv,
    .wipe     = ida_wipe_readv,
};

void ida_execute_readv(ida_local_t * local, fd_t * fd, size_t size, off_t offset, uint32_t flags, dict_t * xdata)
{
    int32_t error;
    uint64_t delta;

    local->args.readv.user_offset = offset;
    local->args.readv.user_size = size;
    error = ida_fd_assign(local, &local->args.readv.fd, fd);

//    gf_log(local->xl->name, GF_LOG_DEBUG, "offset: %lu, size: %lu (%u)", offset, size, local->private->block_size);

    delta = offset % local->private->block_size;
    size += delta;
    local->args.readv.offset = (offset - delta) / local->private->fragments;
    local->args.readv.size = (size + (local->private->block_size - (size + local->private->block_size - 1) % local->private->block_size - 1)) / local->private->fragments;
    local->args.readv.flags = flags;
    if ((error == 0) && local->healing && ((delta != 0) || (local->args.readv.size * local->private->fragments != size)))
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Healing request with invalid offset/size");

        error = EINVAL;
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.readv.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_execute(local);
    }
}

static void ida_callback_readv(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(readv, local->next, -1, error, NULL, 0, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(readv, local->next, args->result, args->code, args->readv.buffer.vectors, args->readv.buffer.count, &args->readv.attr, args->readv.buffer.buffers, args->readv.xdata);
    }
}

int32_t ida_nest_readv(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, size_t size, off_t offset, uint32_t flags, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_readv, required, mask, callback);
    if (error == 0)
    {
        ida_execute_readv(new_local, fd, size, offset, flags, xdata);
    }

    return error;
}

int32_t ida_readv(call_frame_t * frame, xlator_t * this, fd_t * fd, size_t size, off_t offset, uint32_t flags, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_readv, 0, 0, ida_callback_readv);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(readv, frame, -1, error, NULL, 0, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_readv(local, fd, size, offset, flags, xdata);
    }

    return 0;
}
