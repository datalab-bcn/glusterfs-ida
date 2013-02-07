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

static int32_t ida_dispatch_fsync_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct iatt * attr_pre, struct iatt * attr_post, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    args->fsync.syncdir = 0;

    error = 0;

    if (result >= 0)
    {
        error = ida_iatt_assign(local, &args->fsync.attr_pre, attr_pre);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->fsync.attr_post, attr_post);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->fsync.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_fsyncdir_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    args->fsync.syncdir = 1;

    error = ida_dict_assign(local, &args->fsync.xdata, xdata);

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_fsync(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    if (local->args.fsync.syncdir == 0)
    {
        IDA_WIND(fsync, frame, child, index, ida_dispatch_fsync_cbk, args->fsync.fd, args->fsync.datasync, args->fsync.xdata);
    }
    else
    {
        IDA_WIND(fsyncdir, frame, child, index, ida_dispatch_fsyncdir_cbk, args->fsync.fd, args->fsync.datasync, args->fsync.xdata);
    }

    return 0;
}

static int32_t ida_combine_fsync(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if ((dst->result >= 0) && (local->args.fsync.syncdir == 0))
    {
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->fsync.attr_pre, &src->fsync.attr_pre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->fsync.attr_post, &src->fsync.attr_post);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->fsync.xdata, src->fsync.xdata);
    }

    return error;
}

static int32_t ida_rebuild_fsync(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    ida_iatt_adjust(local, &args->fsync.attr_pre, args->fsync.xdata, local->args.fsync.fd->inode);
    ida_iatt_adjust(local, &args->fsync.attr_post, args->fsync.xdata, local->args.fsync.fd->inode);

    return 0;
}

static void ida_finish_fsync(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_fsync(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_fd_unassign(&local->args.fsync.fd);
    ida_dict_unassign(&local->args.fsync.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_iatt_unassign(&args->fsync.attr_pre);
        ida_iatt_unassign(&args->fsync.attr_post);
        ida_dict_unassign(&args->fsync.xdata);
    }
}

static ida_manager_t ida_manager_fsync =
{
    .name     = "fsync",
    .dispatch = ida_dispatch_fsync,
    .combine  = ida_combine_fsync,
    .rebuild  = ida_rebuild_fsync,
    .finish   = ida_finish_fsync,
    .wipe     = ida_wipe_fsync,
};

static void ida_execute_fsync(ida_local_t * local, fd_t * fd, int32_t datasync, int32_t syncdir, dict_t * xdata)
{
    int32_t error;

    local->args.fsync.syncdir = syncdir;
    local->args.fsync.datasync = datasync;
    error = ida_fd_assign(local, &local->args.fsync.fd, fd);
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.fsync.xdata, xdata);
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

static void ida_callback_fsync(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        if (args->fsync.syncdir == 0)
        {
            IDA_UNWIND(fsync, local->next, -1, error, NULL, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(fsyncdir, local->next, -1, error, NULL);
        }
    }
    else
    {
        if (args->fsync.syncdir == 0)
        {
            IDA_UNWIND(fsync, local->next, args->result, args->code, &args->fsync.attr_pre, &args->fsync.attr_post, args->fsync.xdata);
        }
        else
        {
            IDA_UNWIND(fsyncdir, local->next, args->result, args->code, args->fsync.xdata);
        }
    }
}

int32_t ida_nest_fsync(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, int32_t datasync, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_fsync, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_fsync(new_local, fd, datasync, 0, xdata);
    }

    return error;
}

int32_t ida_fsync(call_frame_t * frame, xlator_t * this, fd_t * fd, int32_t datasync, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_fsync, 0, 0, ida_callback_fsync);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(fsync, frame, -1, error, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_fsync(local, fd, datasync, 0, xdata);
    }

    return 0;
}

int32_t ida_nest_fsyncdir(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, int32_t datasync, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_fsync, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_fsync(new_local, fd, datasync, 1, xdata);
    }

    return error;
}

int32_t ida_fsyncdir(call_frame_t * frame, xlator_t * this, fd_t * fd, int32_t datasync, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_fsync, 0, 0, ida_callback_fsync);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(fsyncdir, frame, -1, error, NULL);
    }
    else
    {
        ida_execute_fsync(local, fd, datasync, 1, xdata);
    }

    return 0;
}
