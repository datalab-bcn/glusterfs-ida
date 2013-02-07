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
#include "ida-type-statvfs.h"
#include "ida-check.h"
#include "ida-manager.h"

static int32_t ida_dispatch_statfs_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct statvfs * stat, dict_t * xdata)
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
        error = ida_statvfs_assign(local, &args->statfs.stat, stat);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->statfs.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_statfs(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(statfs, frame, child, index, ida_dispatch_statfs_cbk, &args->statfs.loc, args->statfs.xdata);

    return 0;
}

static int32_t ida_combine_statfs(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_statvfs_combine(local, &dst->statfs.stat, &src->statfs.stat);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->statfs.xdata, src->statfs.xdata);
    }

    return error;
}

static int32_t ida_rebuild_statfs(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    uint64_t f;

    f = args->statfs.stat.f_bsize >> 9;

    args->statfs.stat.f_blocks = (args->statfs.stat.f_blocks * local->private->fragments) / f;
    args->statfs.stat.f_bfree = (args->statfs.stat.f_bfree * local->private->fragments) / f;
    args->statfs.stat.f_bavail = (args->statfs.stat.f_bavail * local->private->fragments) / f;

    return 0;
}

static void ida_finish_statfs(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_statfs(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.statfs.loc);
    ida_dict_unassign(&local->args.statfs.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_statvfs_unassign(&args->statfs.stat);
        ida_dict_unassign(&args->statfs.xdata);
    }
}

static ida_manager_t ida_manager_statfs =
{
    .name     = "statfs",
    .dispatch = ida_dispatch_statfs,
    .combine  = ida_combine_statfs,
    .rebuild  = ida_rebuild_statfs,
    .finish   = ida_finish_statfs,
    .wipe     = ida_wipe_statfs,
};

void ida_execute_statfs(ida_local_t * local, loc_t * loc, dict_t * xdata)
{
    int32_t error;

    error = ida_loc_assign(local, &local->args.statfs.loc, loc);
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.statfs.xdata, xdata);
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

static void ida_callback_statfs(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(statfs, local->next, -1, error, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(statfs, local->next, args->result, args->code, &args->statfs.stat, args->statfs.xdata);
    }
}

int32_t ida_nest_statfs(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_statfs, required, mask, callback);
    if (error == 0)
    {
        ida_execute_statfs(new_local, loc, xdata);
    }

    return error;
}

int32_t ida_statfs(call_frame_t * frame, xlator_t * this, loc_t * loc, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_statfs, IDA_EXECUTE_MAX, 0, ida_callback_statfs);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(statfs, frame, -1, error, NULL, NULL);
    }
    else
    {
        ida_execute_statfs(local, loc, xdata);
    }

    return 0;
}
