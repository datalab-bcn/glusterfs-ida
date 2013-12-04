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

static int32_t ida_dispatch_rmdir_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct iatt * attr_ppre, struct iatt * attr_ppost, dict_t * xdata)
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
        error = ida_iatt_assign(local, &args->rmdir.attr_ppre, attr_ppre);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->rmdir.attr_ppost, attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->rmdir.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_rmdir(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(rmdir, frame, child, index, ida_dispatch_rmdir_cbk, &args->rmdir.loc, args->rmdir.flags, args->rmdir.xdata);

    return 0;
}

static int32_t ida_combine_rmdir(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_iatt_combine(local, &dst->rmdir.attr_ppre, &src->rmdir.attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->rmdir.attr_ppost, &src->rmdir.attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->rmdir.xdata, src->rmdir.xdata);
    }

    return error;
}

static int32_t ida_rebuild_rmdir(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_rmdir(ida_local_t * local)
{
    if (local->args.rmdir.unlock)
    {
        ida_flow_entry_unlock(local, NULL, &local->args.rmdir.loc, NULL, local->args.rmdir.loc.name);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_rmdir(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.rmdir.loc);
    ida_dict_unassign(&local->args.rmdir.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_iatt_unassign(&args->rmdir.attr_ppre);
        ida_iatt_unassign(&args->rmdir.attr_ppost);
        ida_dict_unassign(&args->rmdir.xdata);
    }
}

static ida_manager_t ida_manager_rmdir =
{
    .name     = "rmdir",
    .dispatch = ida_dispatch_rmdir,
    .combine  = ida_combine_rmdir,
    .rebuild  = ida_rebuild_rmdir,
    .finish   = ida_finish_rmdir,
    .wipe     = ida_wipe_rmdir,
};

void ida_execute_rmdir(ida_local_t * local, loc_t * loc, int32_t flags, dict_t * xdata)
{
    int32_t error;

    local->args.rmdir.flags = flags;
    error = ida_loc_assign(local, &local->args.rmdir.loc, loc);
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.rmdir.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.rmdir.unlock = 1;
        ida_flow_entry_lock(local, NULL, &local->args.rmdir.loc, NULL, local->args.rmdir.loc.name);
    }
}

static void ida_callback_rmdir(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(rmdir, local->next, -1, error, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(rmdir, local->next, args->result, args->code, &args->rmdir.attr_ppre, &args->rmdir.attr_ppost, args->rmdir.xdata);
    }
}

int32_t ida_nest_rmdir(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, int32_t flags, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_rmdir, required, mask, callback);
    if (error == 0)
    {
        ida_execute_rmdir(new_local, loc, flags, xdata);
    }

    return error;
}

int32_t ida_rmdir(call_frame_t * frame, xlator_t * this, loc_t * loc, int32_t flags, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_rmdir, IDA_EXECUTE_MAX, 0, ida_callback_rmdir);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(rmdir, frame, -1, error, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_rmdir(local, loc, flags, xdata);
    }

    return 0;
}
