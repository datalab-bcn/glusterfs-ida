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

static int32_t ida_dispatch_create_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, fd_t * fd, inode_t * inode, struct iatt * attr, struct iatt * attr_ppre, struct iatt * attr_ppost, dict_t * xdata)
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
        error = ida_fd_assign(local, &args->create.fd, fd);
        if (likely(error == 0))
        {
            error = ida_inode_assign(local, &args->create.inode, inode);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->create.attr, attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->create.attr_ppre, attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->create.attr_ppost, attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->create.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_create(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(create, frame, child, index, ida_dispatch_create_cbk, &args->create.loc, args->create.flags, args->create.mode, args->create.umask, args->create.fd, args->create.xdata);

    return 0;
}

static int32_t ida_combine_create(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_fd_combine(local, dst->create.fd, src->create.fd);
        }
        if (likely(error == 0))
        {
            error = ida_inode_combine(local, dst->create.inode, src->create.inode);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->create.attr, &src->create.attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->create.attr_ppre, &src->create.attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->create.attr_ppost, &src->create.attr_ppost);
        }

    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->create.xdata, src->create.xdata);
    }

    return error;
}

static int32_t ida_rebuild_create(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_create(ida_local_t * local)
{
    if (local->args.create.unlock)
    {
        ida_flow_entry_unlock(local, NULL, &local->args.create.loc, NULL, local->args.create.loc.name);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_create(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.create.loc);
    ida_fd_unassign(&local->args.create.fd);
    ida_dict_unassign(&local->args.create.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_fd_unassign(&args->create.fd);
        ida_inode_unassign(&args->create.inode);
        ida_iatt_unassign(&args->create.attr);
        ida_iatt_unassign(&args->create.attr_ppre);
        ida_iatt_unassign(&args->create.attr_ppost);
        ida_dict_unassign(&args->create.xdata);
    }
}

static ida_manager_t ida_manager_create =
{
    .name     = "create",
    .dispatch = ida_dispatch_create,
    .combine  = ida_combine_create,
    .rebuild  = ida_rebuild_create,
    .finish   = ida_finish_create,
    .wipe     = ida_wipe_create,
};

void ida_execute_create(ida_local_t * local, loc_t * loc, int32_t flags, mode_t mode, mode_t umask, fd_t * fd, dict_t * xdata)
{
    int32_t error;

    if ((flags & O_ACCMODE) == O_WRONLY)
    {
        flags = (flags & ~O_ACCMODE) | O_RDWR;
    }
    flags &= ~O_APPEND;
    // Force creation flags
    flags |= O_CREAT | O_TRUNC;
    local->args.create.flags = flags;
    local->args.create.mode = mode;
    local->args.create.umask = umask;
    error = ida_loc_assign(local, &local->args.create.loc, loc);
    if (likely(error == 0))
    {
        error = ida_fd_assign(local, &local->args.create.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.create.xdata, xdata);
    }
    if (likely(error == 0))
    {
        error = ida_dict_set_uint64_cow(&local->args.create.xdata, IDA_KEY_VERSION, 0);
    }
    if (likely(error == 0))
    {
        error = ida_dict_set_uint64_cow(&local->args.create.xdata, IDA_KEY_SIZE, 0);
    }

    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.create.unlock = 1;
//        gf_log(local->xl->name, GF_LOG_DEBUG, "create: '%s', '%s'", local->args.create.loc.path, local->args.create.loc.name);
//        gf_log(local->xl->name, GF_LOG_DEBUG, "create: %02X %02X %02X", local->args.create.loc.gfid[0], local->args.create.loc.gfid[1], local->args.create.loc.gfid[2]);
//        gf_log(local->xl->name, GF_LOG_DEBUG, "create: local=%p, private=%p, nodes=%u", local, local->private, local->private->nodes);
        ida_flow_entry_lock(local, NULL, &local->args.create.loc, NULL, local->args.create.loc.name);
    }
}

static void ida_callback_create(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(create, local->next, -1, error, NULL, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(create, local->next, args->result, args->code, args->create.fd, args->create.inode, &args->create.attr, &args->create.attr_ppre, &args->create.attr_ppost, args->create.xdata);
    }
}

int32_t ida_nest_create(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, int32_t flags, mode_t mode, mode_t umask, fd_t * fd, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_create, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_create(new_local, loc, flags, mode, umask, fd, xdata);
    }

    return error;
}

int32_t ida_create(call_frame_t * frame, xlator_t * this, loc_t * loc, int32_t flags, mode_t mode, mode_t umask, fd_t * fd, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_create, IDA_EXECUTE_MAX, 0, ida_callback_create);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(create, frame, -1, error, NULL, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_create(local, loc, flags, mode, umask, fd, xdata);
    }

    return 0;
}
