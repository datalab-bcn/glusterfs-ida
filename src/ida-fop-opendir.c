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
#include "ida-memory.h"
#include "ida-manager.h"

static int32_t ida_dispatch_opendir_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, fd_t * fd, dict_t * xdata)
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
        error = ida_fd_assign(local, &args->opendir.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->opendir.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_opendir(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(opendir, frame, child, index, ida_dispatch_opendir_cbk, &args->opendir.loc, args->opendir.fd, args->opendir.xdata);

    return 0;
}

static int32_t ida_combine_opendir(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_fd_combine(local, dst->opendir.fd, src->opendir.fd);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->opendir.xdata, src->opendir.xdata);
    }

    return error;
}

static int32_t ida_rebuild_opendir(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    ida_dir_ctx_t * ctx;

    ctx = IDA_MALLOC_OR_RETURN_ERROR(local->xl->name, ida_dir_ctx_t, -ENOMEM);
    memset(ctx, 0, sizeof(ida_dir_ctx_t));
    INIT_LIST_HEAD(&ctx->entries.entries.list);

    if (unlikely(fd_ctx_set(args->opendir.fd, local->xl, (uintptr_t)ctx) != 0))
    {
        GF_FREE(ctx);

        return -EIO;
    }

    return 0;
}

static void ida_finish_opendir(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_opendir(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.opendir.loc);
    ida_fd_unassign(&local->args.opendir.fd);
    ida_dict_unassign(&local->args.opendir.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_fd_unassign(&args->opendir.fd);
        ida_dict_unassign(&args->opendir.xdata);
    }
}

static ida_manager_t ida_manager_opendir =
{
    .name     = "opendir",
    .dispatch = ida_dispatch_opendir,
    .combine  = ida_combine_opendir,
    .rebuild  = ida_rebuild_opendir,
    .finish   = ida_finish_opendir,
    .wipe     = ida_wipe_opendir,
};

void ida_execute_opendir(ida_local_t * local, loc_t * loc, fd_t * fd, dict_t * xdata)
{
    int32_t error;

    error = ida_loc_assign(local, &local->args.opendir.loc, loc);
    if (likely(error == 0))
    {
        error = ida_fd_assign(local, &local->args.opendir.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.opendir.xdata, xdata);
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

static void ida_callback_opendir(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(opendir, local->next, -1, error, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(opendir, local->next, args->result, args->code, args->opendir.fd, args->opendir.xdata);
    }
}

int32_t ida_nest_opendir(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, fd_t * fd, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_opendir, required, mask, callback);
    if (error == 0)
    {
        ida_execute_opendir(new_local, loc, fd, xdata);
    }

    return error;
}

int32_t ida_opendir(call_frame_t * frame, xlator_t * this, loc_t * loc, fd_t * fd, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_opendir, 0, 0, ida_callback_opendir);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(opendir, frame, -1, error, NULL, NULL);
    }
    else
    {
        ida_execute_opendir(local, loc, fd, xdata);
    }

    return 0;
}
