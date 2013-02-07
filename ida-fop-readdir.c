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
#include "ida-type-dirent.h"
#include "ida-check.h"
#include "ida-manager.h"

int32_t ida_readdir_select(ida_local_t * local, ida_dir_ctx_t * ctx, off_t offset, off_t size, ida_args_cbk_t * args)
{
    gf_dirent_t * entry, * copy;
    int32_t count;
    off_t total;
    uint64_t i;

    INIT_LIST_HEAD(&args->readdir.entries.entries.list);

    if (list_empty(&ctx->entries.entries.list))
    {
        args->result = 0;

        return 0;
    }

    if (unlikely(offset != ctx->current_offset))
    {
        entry = &ctx->entries.entries;
        for (i = 0; i < offset; i++)
        {
            if (entry->list.next == &ctx->entries.entries.list)
            {
                break;
            }
            entry = list_entry(entry->list.next, gf_dirent_t, list);
        }
    }
    else
    {
        entry = ctx->current;
    }

    count = 0;
    total = 0;
    while (entry->list.next != &ctx->entries.entries.list)
    {
        entry = list_entry(entry->list.next, gf_dirent_t, list);
        total += sizeof(gf_dirent_t) + entry->d_len + 1;
        if (total > size)
        {

            break;
        }

        copy = ida_dirent_dup(local, entry);
        if (unlikely(copy == NULL))
        {
            return ENOMEM;
        }
        list_add_tail(&copy->list, &args->readdir.entries.entries.list);

        ctx->current_offset = entry->d_off;
        ctx->current = entry;

        count++;
    }

    args->result = count;

    return 0;
}

static int32_t ida_dispatch_readdir_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, gf_dirent_t * entries, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    ida_args_t tmp;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = 0;

    if (result >= 0)
    {
        error = ida_dirent_assign(local, &args->readdir.entries, entries);
        if (likely(error == 0))
        {
            if (!list_empty(&entries->list))
            {
                tmp.readdir.fd = local->args.readdir.fd;
                tmp.readdir.size = local->args.readdir.size;
                tmp.readdir.offset = list_entry(entries->list.prev, gf_dirent_t, list)->d_off;
                tmp.readdir.xdata = local->args.readdir.xdata;
                tmp.readdir.readdirp = local->args.readdir.readdirp;

                error = ida_dispatch(local, args->index, &tmp);
                if (likely(error == 0))
                {
                    return 0;
                }
            }
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->readdir.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_readdir(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    if (local->args.readdir.readdirp)
    {
        IDA_WIND(readdirp, frame, child, index, ida_dispatch_readdir_cbk, args->readdir.fd, args->readdir.size, args->readdir.offset, args->readdir.xdata);
    }
    else
    {
        IDA_WIND(readdir, frame, child, index, ida_dispatch_readdir_cbk, args->readdir.fd, args->readdir.size, args->readdir.offset, args->readdir.xdata);
    }

    return 0;
}

static int32_t ida_combine_readdir(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;

    if (unlikely((dst->result < 0) != (src->result < 0)))
    {
        error = EINVAL;
    }
    if (dst->result >= 0)
    {
        if (likely(error == 0))
        {
            if (unlikely(dst->code != src->code))
            {
                error = EINVAL;
            }
        }
        if (likely(error == 0))
        {
            error = ida_dirent_combine(local, &dst->readdir.entries, &src->readdir.entries);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->readdir.xdata, src->readdir.xdata);
    }

    return error;
}

static int32_t ida_rebuild_readdir(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    ida_dir_ctx_t * ctx;
    uint64_t value;
    int32_t error;

    error = 0;
    if (args->result >= 0)
    {
        ida_dirent_clean(local, &args->readdir.entries, local->private->fragments);

        if (unlikely(fd_ctx_get(local->args.readdir.fd, local->xl, &value) != 0) || unlikely(value == 0))
        {
            return -EIO;
        }
        ctx = (ida_dir_ctx_t *)(uintptr_t)value;

        args->readdir.entries.entries.list.next->prev = &ctx->entries.entries.list;
        args->readdir.entries.entries.list.prev->next = &ctx->entries.entries.list;
        ctx->entries.entries.list.next = args->readdir.entries.entries.list.next;
        ctx->entries.entries.list.prev = args->readdir.entries.entries.list.prev;
        ctx->entries.count = args->readdir.entries.count;

        INIT_LIST_HEAD(&args->readdir.entries.entries.list);
        args->readdir.entries.count = 0;

        ctx->current = &ctx->entries.entries;
        ctx->current_offset = 0;

        ctx->cached = 1;

        error = -ida_readdir_select(local, ctx, local->args.readdir.offset, local->args.readdir.size, args);
    }

    return error;
}

static void ida_finish_readdir(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_readdir(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_fd_unassign(&local->args.readdir.fd);
    ida_dict_unassign(&local->args.readdir.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_dirent_unassign(&args->readdir.entries);
        ida_dict_unassign(&args->readdir.xdata);
    }
}

static ida_manager_t ida_manager_readdir =
{
    .name     = "readdir",
    .dispatch = ida_dispatch_readdir,
    .combine  = ida_combine_readdir,
    .rebuild  = ida_rebuild_readdir,
    .finish   = ida_finish_readdir,
    .wipe     = ida_wipe_readdir,
};

void ida_execute_readdir(ida_local_t * local, fd_t * fd, size_t size, off_t offset, dict_t * xdata)
{
    ida_dir_ctx_t * ctx;
    uint64_t value;
    int32_t error;

    gf_log(local->xl->name, GF_LOG_DEBUG, "READDIR: offset=%lu", offset);

    local->args.readdir.size = size;
    local->args.readdir.offset = offset;
    error = ida_fd_assign(local, &local->args.readdir.fd, fd);
    if (likely(error == 0) && (local->args.readdir.xdata == NULL))
    {
        error = ida_dict_assign(local, &local->args.readdir.xdata, xdata);
    }

    if (unlikely(error != 0))
    {
        goto failed;
    }

    if (unlikely(fd_ctx_get(fd, local->xl, &value) != 0) || (value == 0))
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Context not present in readdir");

        error = EIO;

        goto failed;
    }

    ctx = (ida_dir_ctx_t *)(uintptr_t)value;
    if (ctx->cached == 0)
    {
        ida_execute(local);

        return;
    }
    INIT_LIST_HEAD(&local->args_cbk[0].readdir.entries.entries.list);
    error = ida_readdir_select(local, ctx, offset, size, &local->args_cbk[0]);
    if (error == 0)
    {
        ida_report(local, 0, 0, &local->args_cbk[0]);
    }
    ida_dirent_unassign(&local->args_cbk[0].readdir.entries);
    ida_dict_unassign(&local->args_cbk[0].readdir.xdata);

failed:
    ida_complete(local, error);
}

void ida_execute_readdirp(ida_local_t * local, fd_t * fd, size_t size, off_t offset, dict_t * xdata)
{
    int32_t error;

    local->args.readdir.readdirp = 1;

    error = ida_dict_new(local, &local->args.readdir.xdata, xdata);
    if (likely(error == 0))
    {
        error = ida_dict_set_uint64_cow(&local->args.readdir.xdata, IDA_KEY_SIZE, 0);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_execute_readdir(local, fd, size, offset, NULL);
    }
}

static void ida_callback_readdir(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(readdir, local->next, -1, error, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(readdir, local->next, args->result, args->code, &args->readdir.entries.entries, args->readdir.xdata);
    }
}

int32_t ida_nest_readdir(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, size_t size, off_t offset, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_readdir, required, mask, callback);
    if (error == 0)
    {
        ida_execute_readdir(new_local, fd, size, offset, xdata);
    }

    return error;
}

int32_t ida_readdir(call_frame_t * frame, xlator_t * this, fd_t * fd, size_t size, off_t offset, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_readdir, IDA_EXECUTE_MAX, 0, ida_callback_readdir);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(readdir, frame, -1, error, NULL, NULL);
    }
    else
    {
        ida_execute_readdir(local, fd, size, offset, xdata);
    }

    return 0;
}

int32_t ida_nest_readdirp(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, size_t size, off_t offset, dict_t * params)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_readdir, required, mask, callback);
    if (error == 0)
    {
        ida_execute_readdirp(new_local, fd, size, offset, params);
    }

    return error;
}

int32_t ida_readdirp(call_frame_t * frame, xlator_t * this, fd_t * fd, size_t size, off_t offset, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_readdir, IDA_EXECUTE_MAX, 0, ida_callback_readdir);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(readdirp, frame, -1, error, NULL, NULL);
    }
    else
    {
        ida_execute_readdirp(local, fd, size, offset, xdata);
    }

    return 0;
}
