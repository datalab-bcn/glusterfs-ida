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

static int32_t ida_dispatch_link_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, inode_t * inode, struct iatt * attr, struct iatt * attr_ppre, struct iatt * attr_ppost, dict_t * xdata)
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
        error = ida_inode_assign(local, &args->link.inode, inode);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->link.attr, attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->link.attr_ppre, attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->link.attr_ppost, attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->link.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_link(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(link, frame, child, index, ida_dispatch_link_cbk, &args->link.old_loc, &args->link.new_loc, args->link.xdata);

    return 0;
}

static int32_t ida_combine_link(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_inode_combine(local, dst->link.inode, src->link.inode);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->link.attr, &src->link.attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->link.attr_ppre, &src->link.attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->link.attr_ppost, &src->link.attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &dst->link.xdata, src->link.xdata);
    }

    return error;
}

static int32_t ida_rebuild_link(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_link2(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    local = local->next->local;

    if (local->args.link.unlock)
    {
        ida_flow_inode_unlock(local, NULL, &local->args.link.new_loc, NULL, 0, 0);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_finish_link(ida_local_t * local)
{
    if (local->args.link.unlock2)
    {
        ida_flow_inode_unlock(local, ida_finish_link2, &local->args.link.old_loc, NULL, 0, 0);
    }
    else
    {
        ida_finish_link2(local, 0, 0, NULL);
    }
}

static void ida_wipe_link(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.link.old_loc);
    ida_loc_unassign(&local->args.link.new_loc);
    ida_dict_unassign(&local->args.link.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_inode_unassign(&args->link.inode);
        ida_iatt_unassign(&args->link.attr);
        ida_iatt_unassign(&args->link.attr_ppre);
        ida_iatt_unassign(&args->link.attr_ppost);
        ida_dict_unassign(&args->link.xdata);
    }
}

static ida_manager_t ida_manager_link =
{
    .name     = "link",
    .dispatch = ida_dispatch_link,
    .combine  = ida_combine_link,
    .rebuild  = ida_rebuild_link,
    .finish   = ida_finish_link,
    .wipe     = ida_wipe_link,
};

static void ida_execute_link2(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    local = local->next->local;

    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.link.unlock2 = 1;
        ida_flow_inode_lock(local, NULL, &local->args.link.new_loc, NULL, 0, 0);
    }
}

void ida_execute_link(ida_local_t * local, loc_t * old_loc, loc_t * new_loc, dict_t * xdata)
{
    int32_t error;

    error = ida_loc_assign(local, &local->args.link.old_loc, old_loc);
    if (likely(error == 0))
    {
        error = ida_loc_assign(local, &local->args.link.new_loc, new_loc);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.link.xdata, xdata);
    }

    gf_log(local->xl->name, GF_LOG_DEBUG, "link: healing=%u", local->healing);
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else if (!local->healing)
    {
        local->args.link.unlock = 1;
        ida_flow_inode_lock(local, ida_execute_link2, &local->args.link.old_loc, NULL, 0, 0);
    }
    else
    {
        ida_execute(local);
    }
}

static void ida_callback_link(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(link, local->next, -1, error, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(link, local->next, args->result, args->code, args->link.inode, &args->link.attr, &args->link.attr_ppre, &args->link.attr_ppost, args->link.xdata);
    }
}

int32_t ida_nest_link(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * old_loc, loc_t * new_loc, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, new_loc->inode, &ida_manager_link, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_link(new_local, old_loc, new_loc, xdata);
    }

    return error;
}

int32_t ida_link(call_frame_t * frame, xlator_t * this, loc_t * old_loc, loc_t * new_loc, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, new_loc->inode, &ida_manager_link, 0, 0, ida_callback_link);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(link, frame, -1, error, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_link(local, old_loc, new_loc, xdata);
    }

    return 0;
}
