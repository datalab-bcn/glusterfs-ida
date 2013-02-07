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
#include "ida-check.h"
#include "ida-manager.h"
#include "ida-flow.h"

static int32_t ida_dispatch_truncate_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct iatt * attr_pre, struct iatt * attr_post, dict_t * xdata)
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
        error = ida_iatt_assign(local, &args->truncate.attr_pre, attr_pre);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->truncate.attr_post, attr_post);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->truncate.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_truncate(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    if (local->args.truncate.fd == NULL)
    {
        IDA_WIND(truncate, frame, child, index, ida_dispatch_truncate_cbk, &args->truncate.loc, args->truncate.offset, args->truncate.xdata);
    }
    else
    {
        IDA_WIND(ftruncate, frame, child, index, ida_dispatch_truncate_cbk, args->truncate.fd, args->truncate.offset, args->truncate.xdata);
    }

    return 0;
}

static int32_t ida_combine_truncate(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_iatt_combine(local, &dst->truncate.attr_pre, &src->truncate.attr_pre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->truncate.attr_post, &src->truncate.attr_post);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->truncate.xdata, src->truncate.xdata);
    }

    return error;
}

static int32_t ida_rebuild_truncate(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    inode_t * inode;

    if (local->args.truncate.fd == NULL)
    {
        inode = local->args.truncate.loc.inode;
    }
    else
    {
        inode = local->args.truncate.fd->inode;
    }
    ida_iatt_adjust(local, &args->truncate.attr_pre, NULL, inode);
    ida_iatt_adjust(local, &args->truncate.attr_post, NULL, inode);
    args->truncate.attr_post.ia_size = local->args.truncate.user_offset;

    local->inode_ctx->size = args->truncate.attr_post.ia_size;

    return 0;
}

static void ida_finish_truncate(ida_local_t * local)
{
    if (local->args.truncate.unlock)
    {
        ida_flow_xattr_unlock(local, NULL, &local->args.truncate.loc, local->args.truncate.fd, 0, 0, local->cbk->truncate.attr_post.ia_size);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_truncate(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.truncate.loc);
    ida_fd_unassign(&local->args.truncate.fd);
    ida_dict_unassign(&local->args.truncate.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_iatt_unassign(&args->truncate.attr_pre);
        ida_iatt_unassign(&args->truncate.attr_post);
        ida_dict_unassign(&args->truncate.xdata);
    }
}

static ida_manager_t ida_manager_truncate =
{
    .name     = "truncate",
    .dispatch = ida_dispatch_truncate,
    .combine  = ida_combine_truncate,
    .rebuild  = ida_rebuild_truncate,
    .finish   = ida_finish_truncate,
    .wipe     = ida_wipe_truncate,
};

static void ida_execute_truncate(ida_local_t * local, loc_t * loc, fd_t * fd, off_t offset, int32_t lock, dict_t * xdata)
{
    int32_t error;

    local->args.truncate.user_offset = offset;
    offset += local->private->block_size - (offset + local->private->block_size - 1) % local->private->block_size - 1;
    local->args.truncate.offset = offset / local->private->fragments;
    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->args.truncate.loc, loc);
    }
    else
    {
        error = ida_fd_assign(local, &local->args.truncate.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.truncate.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else if (lock)
    {
        local->args.truncate.unlock = 1;
        ida_flow_xattr_lock(local, NULL, &local->args.truncate.loc, local->args.truncate.fd, 0, 0);
    }
    else
    {
        ida_execute(local);
    }
}

static void ida_callback_truncate(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        if (local->args.truncate.fd == NULL)
        {
            IDA_UNWIND(truncate, local->next, -1, error, NULL, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(ftruncate, local->next, -1, error, NULL, NULL, NULL);
        }
    }
    else
    {
        if (local->args.truncate.fd == NULL)
        {
            IDA_UNWIND(truncate, local->next, args->result, args->code, &args->truncate.attr_pre, &args->truncate.attr_post, args->truncate.xdata);
        }
        else
        {
            IDA_UNWIND(ftruncate, local->next, args->result, args->code, &args->truncate.attr_pre, &args->truncate.attr_post, args->truncate.xdata);
        }
    }
}

static int32_t ida_nest_truncate_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, inode_t * inode, loc_t * loc, fd_t * fd, off_t offset, int32_t lock, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, inode, &ida_manager_truncate, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_truncate(new_local, loc, fd, offset, lock, xdata);
    }

    return error;
}

int32_t ida_nest_truncate(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, off_t offset, int32_t lock, dict_t * xdata)
{
    return ida_nest_truncate_common(local, required, mask, callback, loc->inode, loc, NULL, offset, lock, xdata);
}

int32_t ida_nest_ftruncate(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, off_t offset, int32_t lock, dict_t * xdata)
{
    return ida_nest_truncate_common(local, required, mask, callback, fd->inode, NULL, fd, offset, lock, xdata);
}

static int32_t ida_truncate_common(call_frame_t * frame, xlator_t * this, inode_t * inode, loc_t * loc, fd_t * fd, off_t offset, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, inode, &ida_manager_truncate, 0, 0, ida_callback_truncate);
    if (unlikely(error != 0))
    {
        if (fd == NULL)
        {
            IDA_UNWIND(truncate, frame, -1, error, NULL, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(ftruncate, frame, -1, error, NULL, NULL, NULL);
        }
    }
    else
    {
        ida_execute_truncate(local, loc, fd, offset, 1, xdata);
    }

    return 0;
}

int32_t ida_truncate(call_frame_t * frame, xlator_t * this, loc_t * loc, off_t offset, dict_t * xdata)
{
    return ida_truncate_common(frame, this, loc->inode, loc, NULL, offset, xdata);
}

int32_t ida_ftruncate(call_frame_t * frame, xlator_t * this, fd_t * fd, off_t offset, dict_t * xdata)
{
    return ida_truncate_common(frame, this, fd->inode, NULL, fd, offset, xdata);
}
