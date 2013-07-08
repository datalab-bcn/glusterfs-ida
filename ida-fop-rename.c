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

static int32_t ida_dispatch_rename_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct iatt * attr, struct iatt * attr_ppre_old, struct iatt * attr_ppost_old, struct iatt * attr_ppre_new, struct iatt * attr_ppost_new, dict_t * xdata)
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
        error = ida_iatt_assign(local, &args->rename.attr, attr);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->rename.attr_ppre_old, attr_ppre_old);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->rename.attr_ppost_old, attr_ppost_old);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->rename.attr_ppre_new, attr_ppre_new);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->rename.attr_ppost_new, attr_ppost_new);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->rename.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_rename(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(rename, frame, child, index, ida_dispatch_rename_cbk, &args->rename.old_loc, &args->rename.new_loc, args->rename.xdata);

    return 0;
}

static int32_t ida_combine_rename(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            ida_iatt_combine(local, &dst->rename.attr, &src->rename.attr);
        }
        if (likely(error == 0))
        {
            ida_iatt_combine(local, &dst->rename.attr_ppre_old, &src->rename.attr_ppre_old);
        }
        if (likely(error == 0))
        {
            ida_iatt_combine(local, &dst->rename.attr_ppost_old, &src->rename.attr_ppost_old);
        }
        if (likely(error == 0))
        {
            ida_iatt_combine(local, &dst->rename.attr_ppre_new, &src->rename.attr_ppre_new);
        }
        if (likely(error == 0))
        {
            ida_iatt_combine(local, &dst->rename.attr_ppost_new, &src->rename.attr_ppost_new);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->rename.xdata, src->rename.xdata);
    }

    return error;
}

static int32_t ida_rebuild_rename(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_rename2(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    local = local->next->local;

    if (local->args.rename.unlock)
    {
        ida_flow_entry_unlock(local, NULL, &local->args.rename.new_loc, NULL, local->args.rename.new_loc.name);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_finish_rename(ida_local_t * local)
{
    if (local->args.rename.unlock2)
    {
        ida_flow_entry_unlock(local, ida_finish_rename2, &local->args.rename.old_loc, NULL, local->args.rename.old_loc.name);
    }
    else
    {
        ida_finish_rename2(local, 0, 0, NULL);
    }
}

static void ida_wipe_rename(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.rename.old_loc);
    ida_loc_unassign(&local->args.rename.new_loc);
    ida_dict_unassign(&local->args.rename.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_iatt_unassign(&args->rename.attr);
        ida_iatt_unassign(&args->rename.attr_ppre_old);
        ida_iatt_unassign(&args->rename.attr_ppost_old);
        ida_iatt_unassign(&args->rename.attr_ppre_new);
        ida_iatt_unassign(&args->rename.attr_ppost_new);
        ida_dict_unassign(&args->rename.xdata);
    }
}

static ida_manager_t ida_manager_rename =
{
    .name     = "rename",
    .dispatch = ida_dispatch_rename,
    .combine  = ida_combine_rename,
    .rebuild  = ida_rebuild_rename,
    .finish   = ida_finish_rename,
    .wipe     = ida_wipe_rename,
};

static void ida_execute_rename2(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    local = local->next->local;

    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.rename.unlock2 = 1;
        ida_flow_entry_lock(local, NULL, &local->args.rename.new_loc, NULL, local->args.rename.new_loc.name);
    }
}

void ida_execute_rename(ida_local_t * local, loc_t * old_loc, loc_t * new_loc, dict_t * xdata)
{
    int32_t error;

    error = ida_loc_assign(local, &local->args.rename.old_loc, old_loc);
    if (likely(error == 0))
    {
        error = ida_loc_assign(local, &local->args.rename.new_loc, new_loc);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.rename.xdata, xdata);
    }

    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.rename.unlock = 1;
        ida_flow_entry_lock(local, ida_execute_rename2, &local->args.rename.old_loc, NULL, local->args.rename.old_loc.name);
    }
}

static void ida_callback_rename(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(rename, local->next, -1, error, NULL, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(rename, local->next, args->result, args->code, &args->rename.attr, &args->rename.attr_ppre_old, &args->rename.attr_ppost_old, &args->rename.attr_ppre_new, &args->rename.attr_ppost_new, args->rename.xdata);
    }
}

int32_t ida_nest_rename(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * old_loc, loc_t * new_loc, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, old_loc->inode, &ida_manager_rename, required, mask, callback);
    if (error == 0)
    {
        ida_execute_rename(new_local, old_loc, new_loc, xdata);
    }

    return error;
}

int32_t ida_rename(call_frame_t * frame, xlator_t * this, loc_t * old_loc, loc_t * new_loc, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, old_loc->inode, &ida_manager_rename, IDA_EXECUTE_MAX, 0, ida_callback_rename);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(rename, frame, -1, error, NULL, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_rename(local, old_loc, new_loc, xdata);
    }

    return 0;
}
