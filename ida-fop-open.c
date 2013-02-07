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
#include "ida-type-iatt.h"
#include "ida-type-fd.h"
#include "ida-check.h"
#include "ida-manager.h"

static int32_t ida_dispatch_open_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, fd_t * fd, dict_t * xdata)
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
        error = ida_fd_assign(local, &args->open.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->open.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_open(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(open, frame, child, index, ida_dispatch_open_cbk, &args->open.loc, args->open.flags, args->open.fd, args->open.xdata);

    return 0;
}

static int32_t ida_combine_open(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_fd_combine(local, dst->open.fd, src->open.fd);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->open.xdata, src->open.xdata);
    }

    return error;
}

static int32_t ida_rebuild_open(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    int32_t index;

    ida_mask_for_each(index, mask)
    {
        gf_log(local->xl->name, GF_LOG_DEBUG, "index: %u", index);
    }

    return 0;
}

static void ida_finish_open(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_open(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.open.loc);
    ida_fd_unassign(&local->args.open.fd);
    ida_dict_unassign(&local->args.open.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_fd_unassign(&args->open.fd);
        ida_dict_unassign(&args->open.xdata);
    }
}

static ida_manager_t ida_manager_open =
{
    .name     = "open",
    .dispatch = ida_dispatch_open,
    .combine  = ida_combine_open,
    .rebuild  = ida_rebuild_open,
    .finish   = ida_finish_open,
    .wipe     = ida_wipe_open,
};

void ida_execute_open(ida_local_t * local, loc_t * loc, int32_t flags, fd_t * fd, dict_t * xdata)
{
    int32_t error;

    if ((flags & O_ACCMODE) == O_WRONLY)
    {
        flags = (flags & ~O_ACCMODE) | O_RDWR;
    }
    flags &= ~O_APPEND;
    local->args.open.flags = flags;

    error = ida_loc_assign(local, &local->args.open.loc, loc);
    if (likely(error == 0))
    {
        error = ida_fd_assign(local, &local->args.open.fd, fd);
    }
    if (likely(error == 0) && (xdata != NULL))
    {
        error = ida_dict_assign(local, &local->args.open.xdata, xdata);
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

static void ida_callback_open(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(open, local->next, -1, error, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(open, local->next, args->result, args->code, args->open.fd, args->open.xdata);
    }
}

int32_t ida_nest_open(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, int32_t flags, fd_t * fd, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_open, required, mask, callback);
    if (error == 0)
    {
        ida_execute_open(new_local, loc, flags, fd, xdata);
    }

    return error;
}

int32_t ida_open(call_frame_t * frame, xlator_t * this, loc_t * loc, int32_t flags, fd_t * fd, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_open, 0, 0, ida_callback_open);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(open, frame, -1, error, NULL, NULL);
    }
    else
    {
        ida_execute_open(local, loc, flags, fd, xdata);
    }

    return 0;
}
