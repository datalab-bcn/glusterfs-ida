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

static int32_t ida_dispatch_mkdir_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, inode_t * inode, struct iatt * attr, struct iatt * attr_ppre, struct iatt * attr_ppost, dict_t * xdata)
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
        error = ida_inode_assign(local, &args->mkdir.inode, inode);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->mkdir.attr, attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->mkdir.attr_ppre, attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->mkdir.attr_ppost, attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->mkdir.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_mkdir(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(mkdir, frame, child, index, ida_dispatch_mkdir_cbk, &args->mkdir.loc, args->mkdir.mode, args->mkdir.umask, args->mkdir.xdata);

    return 0;
}

static int32_t ida_combine_mkdir(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_inode_combine(local, dst->mkdir.inode, src->mkdir.inode);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->mkdir.attr, &src->mkdir.attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->mkdir.attr_ppre, &src->mkdir.attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->mkdir.attr_ppost, &src->mkdir.attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->mkdir.xdata, src->mkdir.xdata);
    }

    return error;
}

static int32_t ida_rebuild_mkdir(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_mkdir(ida_local_t * local)
{
    if (local->args.mkdir.unlock)
    {
        ida_flow_entry_unlock(local, NULL, &local->args.mkdir.loc, NULL, local->args.mkdir.loc.name);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_mkdir(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.mkdir.loc);
    ida_dict_unassign(&local->args.mkdir.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_inode_unassign(&args->mkdir.inode);
        ida_iatt_unassign(&args->mkdir.attr);
        ida_iatt_unassign(&args->mkdir.attr_ppre);
        ida_iatt_unassign(&args->mkdir.attr_ppost);
        ida_dict_unassign(&args->mkdir.xdata);
    }
}

static ida_manager_t ida_manager_mkdir =
{
    .name     = "mkdir",
    .dispatch = ida_dispatch_mkdir,
    .combine  = ida_combine_mkdir,
    .rebuild  = ida_rebuild_mkdir,
    .finish   = ida_finish_mkdir,
    .wipe     = ida_wipe_mkdir,
};

void ida_execute_mkdir(ida_local_t * local, loc_t * loc, mode_t mode, mode_t umask, dict_t * xdata)
{
    int32_t error;

    local->args.mkdir.mode = mode;
    local->args.mkdir.umask = umask;
    error = ida_loc_assign(local, &local->args.mkdir.loc, loc);
    if (likely(error == 0))
    {
        ida_dict_assign(local, &local->args.mkdir.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.mkdir.unlock = 1;
        ida_flow_entry_lock(local, NULL, &local->args.mkdir.loc, NULL, local->args.mkdir.loc.name);
    }
}

static void ida_callback_mkdir(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(mkdir, local->next, -1, error, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(mkdir, local->next, args->result, args->code, args->mkdir.inode, &args->mkdir.attr, &args->mkdir.attr_ppre, &args->mkdir.attr_ppost, args->mkdir.xdata);
    }
}

int32_t ida_nest_mkdir(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, mode_t mode, mode_t umask, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_mkdir, required, mask, callback);
    if (error == 0)
    {
        ida_execute_mkdir(new_local, loc, mode, umask, xdata);
    }

    return error;
}

int32_t ida_mkdir(call_frame_t * frame, xlator_t * this, loc_t * loc, mode_t mode, mode_t umask, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_mkdir, 0, 0, ida_callback_mkdir);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(mkdir, frame, -1, error, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_mkdir(local, loc, mode, umask, xdata);
    }

    return 0;
}
