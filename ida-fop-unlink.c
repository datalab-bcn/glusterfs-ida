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

static int32_t ida_dispatch_unlink_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct iatt * attr_ppre, struct iatt * attr_ppost, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

//    gf_log(local->xl->name, GF_LOG_DEBUG, "unlink: xl=%p, local->private=%p, nodes=%u, frame=%p, local=%p, frame->local=%p", this, local->private, local->private->nodes, frame, local, frame->local);

    error = 0;

    if (result >= 0)
    {
        error = ida_iatt_assign(local, &args->unlink.attr_ppre, attr_ppre);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->unlink.attr_ppost, attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->unlink.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_unlink(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
//    gf_log(local->xl->name, GF_LOG_DEBUG, "unlink: xl=%p, local->private=%p, nodes=%u, frame=%p, local=%p, frame->local=%p", local->xl, local->private, local->private->nodes, frame, local, frame->local);

    IDA_WIND(unlink, frame, child, index, ida_dispatch_unlink_cbk, &args->unlink.loc, args->unlink.xflags, args->unlink.xdata);

    return 0;
}

static int32_t ida_combine_unlink(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_iatt_combine(local, &dst->unlink.attr_ppre, &src->unlink.attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->unlink.attr_ppost, &src->unlink.attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->unlink.xdata, src->unlink.xdata);
    }

    return error;
}

static int32_t ida_rebuild_unlink(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_unlink(ida_local_t * local)
{
    if (local->args.unlink.unlock)
    {
        ida_flow_entry_unlock(local, NULL, &local->args.unlink.loc, NULL, local->args.unlink.loc.name);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_unlink(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.unlink.loc);
    ida_dict_unassign(&local->args.unlink.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_iatt_unassign(&args->unlink.attr_ppre);
        ida_iatt_unassign(&args->unlink.attr_ppost);
        ida_dict_unassign(&args->unlink.xdata);
    }
}

static ida_manager_t ida_manager_unlink =
{
    .name     = "unlink",
    .dispatch = ida_dispatch_unlink,
    .combine  = ida_combine_unlink,
    .rebuild  = ida_rebuild_unlink,
    .finish   = ida_finish_unlink,
    .wipe     = ida_wipe_unlink,
};

void ida_execute_unlink(ida_local_t * local, loc_t * loc, int xflags, dict_t * xdata)
{
    int32_t error;

    local->args.unlink.xflags = xflags;
    error = ida_loc_assign(local, &local->args.unlink.loc, loc);
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.unlink.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.unlink.unlock = 1;
//        gf_log(local->xl->name, GF_LOG_DEBUG, "unlink: healing=%d", local->healing);
        ida_flow_entry_lock(local, NULL, &local->args.unlink.loc, NULL, local->args.unlink.loc.name);
    }
}

static void ida_callback_unlink(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(unlink, local->next, -1, error, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(unlink, local->next, args->result, args->code, &args->unlink.attr_ppre, &args->unlink.attr_ppost, args->unlink.xdata);
    }
}

int32_t ida_nest_unlink(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, int xflags, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_unlink, required, mask, callback);
    if (error == 0)
    {
        ida_execute_unlink(new_local, loc, xflags, xdata);
    }

    return error;
}

int32_t ida_unlink(call_frame_t * frame, xlator_t * this, loc_t * loc, int xflags, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_unlink, IDA_EXECUTE_MAX, 0, ida_callback_unlink);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(unlink, frame, -1, error, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_unlink(local, loc, xflags, xdata);
    }

    return 0;
}
