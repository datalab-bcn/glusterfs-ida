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
#include "ida-flow.h"
#include "ida-check.h"
#include "ida-memory.h"
#include "ida-manager.h"
#include "ida-rabin.h"

static int32_t ida_dispatch_writev_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct iatt * attr_pre, struct iatt * attr_post, dict_t * xdata)
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
        error = ida_iatt_assign(local, &args->writev.attr_pre, attr_pre);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->writev.attr_post, attr_post);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->writev.xdata, xdata);
    }

//    gf_log(this->name, GF_LOG_DEBUG, "writev: answer from '%s'", this->name);

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_writev(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    ida_args_writev_cbk_t * writev;
    uint32_t size, idx, error;
    uint8_t * ptr;

    writev = &local->args_cbk[index].writev;

    size = iov_length(args->writev.buffer.vectors, args->writev.buffer.count);
    size += local->private->block_size - (size + local->private->block_size - 1) % local->private->block_size - 1;

    error  = ida_buffer_new(local, &writev->buffer, size / local->private->fragments);
    if (unlikely(error != 0))
    {
        return error;
    }

    idx = 0;
    ptr = writev->buffer.vectors->iov_base;
    while (size > args->writev.buffer.vectors[idx].iov_len)
    {
        ida_rabin_split(args->writev.buffer.vectors[idx].iov_len, local->private->fragments, index, args->writev.buffer.vectors[idx].iov_base, ptr);
        ptr += args->writev.buffer.vectors[idx].iov_len / local->private->fragments;
        size -= args->writev.buffer.vectors[idx].iov_len;
        idx++;
    }
    ida_rabin_split(size, local->private->fragments, index, args->writev.buffer.vectors[idx].iov_base, ptr);

//    gf_log(local->xl->name, GF_LOG_DEBUG, "writev: dispatch to '%s'", child->name);

    IDA_WIND(writev, frame, child, index, ida_dispatch_writev_cbk, args->writev.fd, writev->buffer.vectors, writev->buffer.count, args->writev.offset, args->writev.flags, writev->buffer.buffers, args->writev.xdata);

    return 0;
}

static int32_t ida_combine_writev(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_iatt_combine(local, &dst->writev.attr_pre, &src->writev.attr_pre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->writev.attr_post, &src->writev.attr_post);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->writev.xdata, src->writev.xdata);
    }

//    gf_log(local->xl->name, GF_LOG_DEBUG, "writev combine: %d, %d", dst->result, error);

    return error;
}

static int32_t ida_rebuild_writev(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    uint64_t end, real_size;
    ssize_t size;

    if (args->result < 0)
    {
        return 0;
    }

    if (args->writev.attr_post.ia_type == IA_IFREG)
    {
        ida_iatt_adjust(local, &args->writev.attr_pre, args->writev.xdata, local->args.writev.fd->inode);
        ida_iatt_adjust(local, &args->writev.attr_post, args->writev.xdata, local->args.writev.fd->inode);

        size = args->result * local->private->fragments - local->args.writev.head - local->args.writev.tail;
        if (size < 0)
        {
            size = 0;
        }
        else if (size > local->args.writev.user_size)
        {
            size = local->args.writev.user_size;
        }
        end = local->args.writev.user_offset + size;
        if (args->writev.attr_post.ia_size < end)
        {
            args->writev.attr_post.ia_size = end;
        }

        LOCK(&local->inode->lock);

        real_size = local->inode_ctx->size;
        end = args->writev.attr_post.ia_size;
        if (end > real_size)
        {
            local->inode_ctx->size = end;
        }

        UNLOCK(&local->inode->lock);

        args->result = size;
    }

    return 0;
}

static void ida_finish_writev(ida_local_t * local)
{
    off_t size;

    if (local->args.writev.unlock)
    {
        size = iov_length(local->args.writev.buffer.vectors, local->args.writev.buffer.count);
        size += local->private->block_size - (size + local->private->block_size - 1) % local->private->block_size - 1;
        size /= local->private->fragments;

        ida_flow_xattr_unlock(local, NULL, NULL, local->args.writev.fd, local->args.writev.offset, size, local->cbk->writev.attr_post.ia_size);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_writev(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_fd_unassign(&local->args.writev.fd);
    ida_buffer_unassign(&local->args.writev.buffer);
    ida_dict_unassign(&local->args.writev.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_iatt_unassign(&args->writev.attr_pre);
        ida_iatt_unassign(&args->writev.attr_post);
        ida_buffer_unassign(&args->writev.buffer);
        ida_dict_unassign(&args->writev.xdata);
    }
}

static ida_manager_t ida_manager_writev =
{
    .name     = "writev",
    .dispatch = ida_dispatch_writev,
    .combine  = ida_combine_writev,
    .rebuild  = ida_rebuild_writev,
    .finish   = ida_finish_writev,
    .wipe     = ida_wipe_writev,
};

static void _ida_writev_ready(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if ((error != 0) || (args->result < 0))
    {
        ida_complete(local, (error == 0) ? args->result : error);
    }
    else
    {
        ida_execute(local);
    }
}

static void ida_writev_ready(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    int32_t size, buf;
    uint32_t off;

//    gf_log(local->xl->name, GF_LOG_DEBUG, "ida_writev_ready: %s", local->manager.name);

    local = local->next->local;

    if ((error == 0) && (args->result > 0))
    {
        size = args->result + local->args.writev.tail - local->private->block_size;
        if (size > 0)
        {
            buf = local->args.writev.buffer.count - 1;
            off = local->args.writev.user_size + local->args.writev.head - buf * iobuf_pagesize(local->args.writev.buffer.buffers->iobrefs[0]);
            memcpy(local->args.writev.buffer.buffers->iobrefs[buf]->ptr + off, args->readv.buffer.vectors->iov_base + local->private->block_size - local->args.writev.tail, size);
        }
    }

    _ida_writev_ready(local, mask, error, args);
}

static void _ida_writev_tail(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    off_t offset;

    if ((error != 0) || (args->result < 0))
    {
        ida_complete(local, (error == 0) ? args->result : error);
    }
    else
    {
        if (local->args.writev.tail == 0)
        {
            _ida_writev_ready(local, mask, error, args);
        }
        else
        {
            offset = local->args.writev.user_offset + local->args.writev.user_size;
            offset -= local->private->block_size - local->args.writev.tail;
            ida_nest_readv(local, 0, mask, ida_writev_ready, local->args.writev.fd, local->private->block_size, offset, 0, NULL);
        }
    }
}

static void ida_writev_tail(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * new_local;
    uint32_t len;

//    gf_log(local->xl->name, GF_LOG_DEBUG, "ida_writev_tail: %s", local->manager.name);

    new_local = local->next->local;

    if ((error == 0) && (args->result >= 0))
    {
        len = new_local->args.writev.user_size;
        if (len > new_local->args.writev.buffer.vectors->iov_len - new_local->args.writev.head)
        {
            len = new_local->args.writev.buffer.vectors->iov_len - new_local->args.writev.head;
            memcpy(new_local->args.writev.buffer.vectors[1].iov_base, new_local->args.writev.buffer.vectors->iov_base + len, new_local->args.writev.user_size - len);
            len -= new_local->args.writev.tail;
        }
        memmove(new_local->args.writev.buffer.buffers->iobrefs[0]->ptr + new_local->args.writev.head, new_local->args.writev.buffer.buffers->iobrefs[0]->ptr, len + new_local->args.writev.tail);
        len = args->result;
        if (len > new_local->args.writev.head)
        {
            len = new_local->args.writev.head;
        }
        memcpy(new_local->args.writev.buffer.buffers->iobrefs[0]->ptr, args->readv.buffer.vectors[0].iov_base, len);
        memset(new_local->args.writev.buffer.buffers->iobrefs[0]->ptr + args->result, 0, new_local->args.writev.head - len);
        if (new_local->args.writev.head + new_local->args.writev.user_size <= new_local->private->block_size)
        {
            ida_writev_ready(local, mask, error, args);

            return;
        }
    }

    _ida_writev_tail(new_local, mask, error, args);
}

static void ida_writev_head(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
//    gf_log(local->xl->name, GF_LOG_DEBUG, "ida_writev_head: %s", local->manager.name);

    local = local->next->local;

    if ((error != 0) || (args->result < 0))
    {
        ida_complete(local, (error == 0) ? args->result : error);
    }
    else
    {
        if (local->args.writev.head == 0)
        {
            _ida_writev_tail(local, mask, error, args);
        }
        else
        {
            ida_nest_readv(local, 0, mask, ida_writev_tail, local->args.writev.fd, local->private->block_size, local->args.writev.offset * local->private->fragments, 0, NULL);
        }
    }
}

void ida_execute_writev(ida_local_t * local, fd_t * fd, struct iovec * vectors, int32_t count, off_t offset, uint32_t flags, struct iobref * buffers, dict_t * xdata)
{
    off_t size;
    int32_t error;
    uint32_t len;

    error = 0;
    if (likely(error == 0))
    {
        error = ida_fd_assign(local, &local->args.writev.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_buffer_assign(local, &local->args.writev.buffer, buffers, vectors, count);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.writev.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.writev.user_offset = offset;
        size = iov_length(vectors, count);
        local->args.writev.user_size = size;

        local->args.writev.head = offset % local->private->block_size;
        size += local->args.writev.head;
        local->args.writev.offset = (offset - local->args.writev.head) / local->private->fragments;
        local->args.writev.tail = local->private->block_size - (size + local->private->block_size - 1) % local->private->block_size - 1;
        size += local->args.writev.tail;
        local->args.writev.flags = flags;

        if (size > iobuf_pagesize(local->args.writev.buffer.buffers->iobrefs[0]))
        {
            error = ida_buffer_add_buffer(local, &local->args.writev.buffer, local->private->block_size * 2);
            if (unlikely(error != 0))
            {
                ida_complete(local, error);

                return;
            }
            memset(local->args.writev.buffer.buffers->iobrefs[1]->ptr, 0, local->private->block_size);
            len = iobuf_pagesize(local->args.writev.buffer.buffers->iobrefs[0]);
            error = ida_buffer_add_vector(local, &local->args.writev.buffer, 1, local->args.writev.buffer.buffers->iobrefs[1]->ptr, size - len);
            if (unlikely(error != 0))
            {
                ida_complete(local, error);

                return;
            }
            local->args.writev.buffer.vectors->iov_len = len;
        }
        else
        {
            local->args.writev.buffer.vectors->iov_len = size;
            memset(local->args.writev.buffer.vectors->iov_base + local->args.writev.user_size, 0, local->args.writev.tail);
        }

        local->args.writev.unlock = 1;

        ida_flow_xattr_lock(local, ida_writev_head, NULL, local->args.writev.fd, local->args.writev.offset, size / local->private->fragments);
    }
}

void ida_execute_writev_heal(ida_local_t * local, fd_t * fd, struct iovec * vectors, int32_t count, off_t offset, uint32_t flags, struct iobref * buffers, dict_t * xdata)
{
    off_t size;
    int32_t error;

    error = 0;
    if (likely(error == 0))
    {
        error = ida_fd_assign(local, &local->args.writev.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_buffer_assign(local, &local->args.writev.buffer, buffers, vectors, count);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.writev.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.writev.user_offset = offset;
        size = iov_length(vectors, count);
        local->args.writev.user_size = size;

        local->args.writev.head = offset % local->private->block_size;
        local->args.writev.offset = offset / local->private->fragments;
        local->args.writev.tail = local->private->block_size - (size + local->private->block_size - 1) % local->private->block_size - 1;
        local->args.writev.flags = flags;
        if ((local->args.writev.head != 0) || (local->args.writev.tail != 0))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Invalid offset/size in healing writev");

            ida_complete(local, EINVAL);
        }
        else
        {
            ida_execute(local);
        }
    }
}

static void ida_callback_writev(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(writev, local->next, -1, error, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(writev, local->next, args->result, args->code, &args->writev.attr_pre, &args->writev.attr_post, args->writev.xdata);
    }
}

int32_t ida_nest_writev(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, struct iovec * vectors, int32_t count, off_t offset, uint32_t flags, struct iobref * buffers, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_writev, required, mask, callback);
    if (error == 0)
    {
        ida_execute_writev(new_local, fd, vectors, count, offset, flags, buffers, xdata);
    }

    return error;
}

int32_t ida_writev(call_frame_t * frame, xlator_t * this, fd_t * fd, struct iovec * vectors, int32_t count, off_t offset, uint32_t flags, struct iobref * buffers, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_writev, IDA_EXECUTE_MAX, 0, ida_callback_writev);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(writev, frame, -1, error, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_writev(local, fd, vectors, count, offset, flags, buffers, xdata);
    }

    return 0;
}

int32_t ida_nest_writev_heal(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, struct iovec * vectors, int32_t count, off_t offset, uint32_t flags, struct iobref * buffers, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_writev, required, mask, callback);
    if (error == 0)
    {
        ida_execute_writev_heal(new_local, fd, vectors, count, offset, flags, buffers, xdata);
    }

    return error;
}
