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
#include "ida-type-lock.h"
#include "ida-type-str.h"
#include "ida-check.h"
#include "ida-manager.h"

static void ida_lock_prepare(ida_local_t * local)
{
    local->pid = local->frame->root->pid;
    local->owner.len = local->frame->root->lk_owner.len;
    memcpy(local->owner.data, local->frame->root->lk_owner.data, local->owner.len);

    local->frame->root->pid = (uintptr_t)local->next;
    set_lk_owner_from_ptr(&local->frame->root->lk_owner, local->next);
}

static void ida_lock_unprepare(ida_local_t * local)
{
    local->frame->root->pid = local->pid;
    local->frame->root->lk_owner.len = local->owner.len;
    memcpy(local->frame->root->lk_owner.data, local->owner.data, local->owner.len);
}

static int32_t ida_dispatch_lk_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct gf_flock * lock, dict_t * xdata)
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
        error = ida_lock_assign(local, &args->lk.lock, lock);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->lk.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_lk(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(lk, frame, child, index, ida_dispatch_lk_cbk, args->lk.fd, args->lk.cmd, &args->lk.lock, args->lk.xdata);

    return 0;
}

static int32_t ida_combine_lk(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_lock_combine(local, &dst->lk.lock, &src->lk.lock);
        }
    }
    if (likely(error != 0))
    {
        error = ida_dict_combine_cow(local, &dst->lk.xdata, src->lk.xdata);
    }

    return error;
}

static int32_t ida_rebuild_lk(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    ida_lock_unprepare(local);

    return 0;
}

static void ida_finish_lk(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_lk(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_fd_unassign(&local->args.lk.fd);
    ida_lock_unassign(&local->args.lk.lock);
    ida_dict_unassign(&local->args.lk.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_lock_unassign(&args->lk.lock);
        ida_dict_unassign(&args->lk.xdata);
    }
}

static ida_manager_t ida_manager_lk =
{
    .name     = "lk",
    .dispatch = ida_dispatch_lk,
    .combine  = ida_combine_lk,
    .rebuild  = ida_rebuild_lk,
    .finish   = ida_finish_lk,
    .wipe     = ida_wipe_lk,
};

static void ida_execute_lk(ida_local_t * local, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    int32_t error;

    local->args.lk.cmd = cmd;
    error = ida_fd_assign(local, &local->args.lk.fd, fd);
    if (likely(error == 0))
    {
        error = ida_lock_assign(local, &local->args.lk.lock, lock);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.lk.xdata, xdata);
    }
    ida_lock_prepare(local);
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_execute(local);
    }
}

static void ida_callback_lk(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(lk, local->next, -1, error, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(lk, local->next, args->result, args->code, &args->lk.lock, args->lk.xdata);
    }
}

int32_t ida_nest_lk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_lk, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_lk(new_local, fd, cmd, lock, xdata);
    }

    return error;
}

int32_t ida_lk(call_frame_t * frame, xlator_t * this, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_lk, 0, 0, ida_callback_lk);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(lk, frame, -1, error, NULL, NULL);
    }
    else
    {
        ida_execute_lk(local, fd, cmd, lock, xdata);
    }

    return 0;
}

static int32_t ida_dispatch_inodelk_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = ida_dict_assign(local, &args->inodelk.xdata, xdata);

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_inodelk(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    if (local->args.inodelk.fd == NULL)
    {
        IDA_WIND(inodelk, frame, child, index, ida_dispatch_inodelk_cbk, args->inodelk.volume, &args->inodelk.loc, args->inodelk.cmd, &args->inodelk.lock, NULL);
    }
    else
    {
        IDA_WIND(finodelk, frame, child, index, ida_dispatch_inodelk_cbk, args->inodelk.volume, args->inodelk.fd, args->inodelk.cmd, &args->inodelk.lock, args->inodelk.xdata);
    }

    return 0;
}

static int32_t ida_combine_inodelk(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->inodelk.xdata, src->inodelk.xdata);
    }
    return error;
}

static int32_t ida_rebuild_inodelk(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    ida_lock_unprepare(local);

    return 0;
}

static void ida_finish_inodelk(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_inodelk(ida_local_t * local)
{
    ida_args_cbk_t * args;
    ida_str_unassign(&local->args.inodelk.volume);
    ida_loc_unassign(&local->args.inodelk.loc);
    ida_fd_unassign(&local->args.inodelk.fd);
    ida_lock_unassign(&local->args.inodelk.lock);
    ida_dict_unassign(&local->args.inodelk.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_dict_unassign(&args->inodelk.xdata);
    }
}

static ida_manager_t ida_manager_inodelk =
{
    .name     = "inodelk",
    .dispatch = ida_dispatch_inodelk,
    .combine  = ida_combine_inodelk,
    .rebuild  = ida_rebuild_inodelk,
    .finish   = ida_finish_inodelk,
    .wipe     = ida_wipe_inodelk,
};

static void ida_execute_inodelk(ida_local_t * local, const char * volume, loc_t * loc, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    int32_t error;

    local->args.inodelk.cmd = cmd;
    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->args.inodelk.loc, loc);
    }
    else
    {
        error = ida_fd_assign(local, &local->args.inodelk.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_str_assign(local, &local->args.inodelk.volume, volume);
    }
    if (likely(error == 0))
    {
        error = ida_lock_assign(local, &local->args.inodelk.lock, lock);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.inodelk.xdata, xdata);
    }
    ida_lock_prepare(local);
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_execute(local);
    }
}

static void ida_callback_inodelk(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        if (local->args.inodelk.fd == NULL)
        {
            IDA_UNWIND(inodelk, local->next, -1, error, NULL);
        }
        else
        {
            IDA_UNWIND(finodelk, local->next, -1, error, NULL);
        }
    }
    else
    {
        if (local->args.inodelk.fd == NULL)
        {
            IDA_UNWIND(inodelk, local->next, args->result, args->code, args->inodelk.xdata);
        }
        else
        {
            IDA_UNWIND(finodelk, local->next, args->result, args->code, args->inodelk.xdata);
        }
    }
}

int32_t ida_nest_inodelk_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, inode_t * inode, loc_t * loc, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, inode, &ida_manager_inodelk, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_inodelk(new_local, volume, loc, fd, cmd, lock, xdata);
    }

    return error;
}

int32_t ida_nest_inodelk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, loc_t * loc, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    return ida_nest_inodelk_common(local, required, mask, callback, volume, loc->inode, loc, NULL, cmd, lock, xdata);
}

int32_t ida_nest_finodelk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    return ida_nest_inodelk_common(local, required, mask, callback, volume, fd->inode, NULL, fd, cmd, lock, xdata);
}

static int32_t ida_inodelk_common(call_frame_t * frame, xlator_t * this, const char * volume, inode_t * inode, loc_t * loc, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, inode, &ida_manager_inodelk, 0, 0, ida_callback_inodelk);
    if (unlikely(error != 0))
    {
        if (fd == NULL)
        {
            IDA_UNWIND(inodelk, frame, -1, error, NULL);
        }
        else
        {
            IDA_UNWIND(finodelk, frame, -1, error, NULL);
        }
    }
    else
    {
        ida_execute_inodelk(local, volume, loc, fd, cmd, lock, xdata);
    }

    return 0;
}

int32_t ida_inodelk(call_frame_t * frame, xlator_t * this, const char * volume, loc_t * loc, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    return ida_inodelk_common(frame, this, volume, loc->inode, loc, NULL, cmd, lock, xdata);
}

int32_t ida_finodelk(call_frame_t * frame, xlator_t * this, const char * volume, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata)
{
    return ida_inodelk_common(frame, this, volume, fd->inode, NULL, fd, cmd, lock, xdata);
}

static int32_t ida_dispatch_entrylk_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }
//    gf_log(this->name, GF_LOG_DEBUG, "entrylk_cbk: local=%p, private=%p, nodes=%u", local, local->private, local->private->nodes);
    error = ida_dict_assign(local, &args->entrylk.xdata, xdata);

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_entrylk(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
//    gf_log(local->xl->name, GF_LOG_DEBUG, "entrylk dispatch: local=%p, private=%p, nodes=%u", local, local->private, local->private->nodes);
    if (local->args.entrylk.fd == NULL)
    {
        IDA_WIND(entrylk, frame, child, index, ida_dispatch_entrylk_cbk, args->entrylk.volume, &args->entrylk.loc, args->entrylk.basename, args->entrylk.cmd, args->entrylk.type, args->entrylk.xdata);
    }
    else
    {
        IDA_WIND(fentrylk, frame, child, index, ida_dispatch_entrylk_cbk, args->entrylk.volume, args->entrylk.fd, args->entrylk.basename, args->entrylk.cmd, args->entrylk.type, args->entrylk.xdata);
    }

    return 0;
}

static int32_t ida_combine_entrylk(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->entrylk.xdata, src->entrylk.xdata);
    }

    return error;
}

static int32_t ida_rebuild_entrylk(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    ida_lock_unprepare(local);

    return 0;
}

static void ida_finish_entrylk(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_entrylk(ida_local_t * local)
{
    ida_args_cbk_t * args;
    ida_str_unassign(&local->args.entrylk.volume);
    ida_loc_unassign(&local->args.entrylk.loc);
    ida_fd_unassign(&local->args.entrylk.fd);
    ida_str_unassign(&local->args.entrylk.basename);
    ida_dict_unassign(&local->args.entrylk.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_dict_unassign(&args->entrylk.xdata);
    }
}

static ida_manager_t ida_manager_entrylk =
{
    .name     = "entrylk",
    .dispatch = ida_dispatch_entrylk,
    .combine  = ida_combine_entrylk,
    .rebuild  = ida_rebuild_entrylk,
    .finish   = ida_finish_entrylk,
    .wipe     = ida_wipe_entrylk,
};

static void ida_execute_entrylk(ida_local_t * local, const char * volume, loc_t * loc, fd_t * fd, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata)
{
    int32_t error;

//    gf_log(local->xl->name, GF_LOG_DEBUG, "entrylk: local=%p, private=%p, nodes=%u", local, local->private, local->private->nodes);
    local->args.entrylk.cmd = cmd;
    local->args.entrylk.type = type;
    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->args.entrylk.loc, loc);
    }
    else
    {
        error = ida_fd_assign(local, &local->args.entrylk.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_str_assign(local, &local->args.entrylk.volume, volume);
    }
    if (likely(error == 0))
    {
        error = ida_str_assign(local, &local->args.entrylk.basename, basename);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.entrylk.xdata, xdata);
    }
    ida_lock_prepare(local);
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_execute(local);
    }
}

static void ida_callback_entrylk(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        if (local->args.entrylk.fd == NULL)
        {
            IDA_UNWIND(entrylk, local->next, -1, error, NULL);
        }
        else
        {
            IDA_UNWIND(fentrylk, local->next, -1, error, NULL);
        }
    }
    else
    {
        if (local->args.entrylk.fd == NULL)
        {
            IDA_UNWIND(entrylk, local->next, args->result, args->code, args->entrylk.xdata);
        }
        else
        {
            IDA_UNWIND(fentrylk, local->next, args->result, args->code, args->entrylk.xdata);
        }
    }
}

int32_t ida_nest_entrylk_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, inode_t * inode, loc_t * loc, fd_t * fd, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, inode, &ida_manager_entrylk, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_entrylk(new_local, volume, loc, fd, basename, cmd, type, xdata);
    }

    return error;
}

int32_t ida_nest_entrylk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, loc_t * loc, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata)
{
    return ida_nest_entrylk_common(local, required, mask, callback, volume, loc->inode, loc, NULL, basename, cmd, type, xdata);
}

int32_t ida_nest_fentrylk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, fd_t * fd, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata)
{
    return ida_nest_entrylk_common(local, required, mask, callback, volume, fd->inode, NULL, fd, basename, cmd, type, xdata);
}

static int32_t ida_entrylk_common(call_frame_t * frame, xlator_t * this, const char * volume, inode_t * inode, loc_t * loc, fd_t * fd, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, inode, &ida_manager_entrylk, 0, 0, ida_callback_entrylk);
    if (unlikely(error != 0))
    {
        if (fd == NULL)
        {
            IDA_UNWIND(entrylk, frame, -1, error, NULL);
        }
        else
        {
            IDA_UNWIND(fentrylk, frame, -1, error, NULL);
        }
    }
    else
    {
        ida_execute_entrylk(local, volume, loc, fd, basename, cmd, type, xdata);
    }

    return 0;
}

int32_t ida_entrylk(call_frame_t * frame, xlator_t * this, const char * volume, loc_t * loc, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata)
{
    return ida_entrylk_common(frame, this, volume, loc->inode, loc, NULL, basename, cmd, type, xdata);
}

int32_t ida_fentrylk(call_frame_t * frame, xlator_t * this, const char * volume, fd_t * fd, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata)
{
    return ida_entrylk_common(frame, this, volume, fd->inode, NULL, fd, basename, cmd, type, xdata);
}
