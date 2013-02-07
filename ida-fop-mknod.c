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
#include "ida-flow.h"
#include "ida-check.h"
#include "ida-manager.h"

static int32_t ida_dispatch_mknod_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, inode_t * inode, struct iatt * attr, struct iatt * attr_ppre, struct iatt * attr_ppost, dict_t * xdata)
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
        error = ida_inode_assign(local, &args->mknod.inode, inode);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->mknod.attr, attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->mknod.attr_ppre, attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->mknod.attr_ppost, attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->mknod.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_mknod(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(mknod, frame, child, index, ida_dispatch_mknod_cbk, &args->mknod.loc, args->mknod.mode, args->mknod.rdev, args->mknod.umask, args->mknod.xdata);

    return 0;
}

static int32_t ida_combine_mknod(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_inode_combine(local, dst->mknod.inode, src->mknod.inode);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->mknod.attr, &src->mknod.attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->mknod.attr_ppre, &src->mknod.attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->mknod.attr_ppost, &src->mknod.attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->mknod.xdata, src->mknod.xdata);
    }

    return error;
}

static int32_t ida_rebuild_mknod(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_mknod(ida_local_t * local)
{
    if (local->args.mknod.unlock)
    {
        ida_flow_entry_unlock(local, NULL, &local->args.mknod.loc, NULL, local->args.mknod.loc.name);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_mknod(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.mknod.loc);
    ida_dict_unassign(&local->args.mknod.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_inode_unassign(&args->mknod.inode);
        ida_iatt_unassign(&args->mknod.attr);
        ida_iatt_unassign(&args->mknod.attr_ppre);
        ida_iatt_unassign(&args->mknod.attr_ppost);
        ida_dict_unassign(&args->mknod.xdata);
    }
}

static ida_manager_t ida_manager_mknod =
{
    .name     = "mknod",
    .dispatch = ida_dispatch_mknod,
    .combine  = ida_combine_mknod,
    .rebuild  = ida_rebuild_mknod,
    .finish   = ida_finish_mknod,
    .wipe     = ida_wipe_mknod,
};

void ida_execute_mknod(ida_local_t * local, loc_t * loc, mode_t mode, dev_t rdev, mode_t umask, dict_t * xdata)
{
    int32_t error;

    local->args.mknod.mode = mode;
    local->args.mknod.rdev = rdev;
    local->args.mknod.umask = umask;
    error = ida_loc_assign(local, &local->args.mknod.loc, loc);
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.mknod.xdata, xdata);
    }

    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.mknod.unlock = 1;
        ida_flow_entry_lock(local, NULL, &local->args.mknod.loc, NULL, local->args.mknod.loc.name);
    }
}

static void ida_callback_mknod(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(mknod, local->next, -1, error, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(mknod, local->next, args->result, args->code, args->mknod.inode, &args->mknod.attr, &args->mknod.attr_ppre, &args->mknod.attr_ppost, args->mknod.xdata);
    }
}

int32_t ida_nest_mknod(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, mode_t mode, dev_t rdev, mode_t umask, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_mknod, required, mask, callback);
    if (error == 0)
    {
        ida_execute_mknod(new_local, loc, mode, rdev, umask, xdata);
    }

    return error;
}

int32_t ida_mknod(call_frame_t * frame, xlator_t * this, loc_t * loc, mode_t mode, dev_t rdev, mode_t umask, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_mknod, 0, 0, ida_callback_mknod);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(mknod, frame, -1, error, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_mknod(local, loc, mode, rdev, umask, xdata);
    }

    return 0;
}
