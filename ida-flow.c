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

#include "ida-check.h"
#include "ida-common.h"
#include "ida-manager.h"
#include "ida-flow.h"
#include "ida-fop-lk.h"
#include "ida-fop-xattr.h"
#include "ida-type-loc.h"
#include "ida-type-fd.h"
#include "ida-type-dict.h"
#include "ida.h"

void ida_flow_unref(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_callback_f callback;
    ida_local_t * local_next;

    local_next = local->next->local;

    if (local_next->flow.callback != NULL)
    {
        callback = local_next->flow.callback;
        local_next->flow.callback = NULL;
        callback(local, mask, error, args);
    }
    else
    {
        ida_unref(local_next);
    }
}

void ida_flow_complete(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_callback_f callback;
    ida_local_t * local_next;

    local_next = local->next->local;

    if (local_next->flow.callback != NULL)
    {
        callback = local_next->flow.callback;
        local_next->flow.callback = NULL;
        callback(local, mask, error, args);
    }
    else
    {
        ida_complete(local_next, error);
    }
}

void ida_flow_execute(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_callback_f callback;
    ida_local_t * local_next;

    local_next = local->next->local;

    if (local_next->flow.callback != NULL)
    {
        callback = local_next->flow.callback;
        local_next->flow.callback = NULL;
        callback(local, mask, error, args);
    }
    else
    {
        ida_execute(local_next);
    }
}

void ida_flow_inode_lock_undone(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * local_next;

    local_next = local->next->local;

    ida_loc_unassign(&local_next->flow.loc);

    ida_flow_complete(local, mask, EIO, args);
}

void ida_flow_inode_lock_done(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * local_next;

    local_next = local->next->local;

    local_next->mask &= mask;

    if (unlikely(error != 0))
    {
        if (ida_bit_count(mask) < local_next->private->fragments)
        {
            ida_flow_inode_unlock(local_next, ida_flow_inode_lock_undone, &local_next->flow.loc, local_next->flow.fd, local_next->flow.inode.offset, local_next->flow.inode.length);

            return;
        }
        error = 0;
    }

    if (unlikely(args->result < 0))
    {
        ida_flow_complete(local, mask, args->code, args);
    }
    else
    {
        ida_flow_execute(local, mask, error, args);
    }

    ida_loc_unassign(&local_next->flow.loc);
}

void ida_flow_inode_lock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, off_t offset, off_t length)
{
    struct gf_flock lock;
    int32_t error;

    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->flow.loc, loc);
    }
    else
    {
        error = ida_fd_to_loc(local, &local->flow.loc, fd);
    }
    if (unlikely(error != 0))
    {
        goto failed;
    }

    local->flow.fd = fd;
    local->flow.callback = callback;
    local->flow.inode.offset = offset;
    local->flow.inode.length = length;

    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
//    lock.l_start = offset;
//    lock.l_len = length;
    // FIXME: It is very bad to lock the entire file, but it is needed to avoid
    //        false conflicts while recombining iatt structures returned by
    //        potentially unordered writes.
    lock.l_start = 0;
    lock.l_len = 0;
    lock.l_owner.len = 0;
    lock.l_pid = 0;

    error = ida_nest_inodelk_common(local, IDA_EXECUTE_MAX, local->mask, ida_flow_inode_lock_done, local->xl->name, local->flow.loc.inode, &local->flow.loc, NULL, F_SETLKW, &lock, NULL);
    if (likely(error == 0))
    {
        return;
    }

    ida_loc_unassign(&local->flow.loc);

failed:
    ida_flow_complete(local, 0, error, NULL);
}

void ida_flow_inode_unlock_done(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * local_next;

    local_next = local->next->local;

    ida_loc_unassign(&local_next->flow.loc);

    ida_flow_unref(local, mask, error, args);
}

void ida_flow_inode_unlock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, off_t offset, off_t length)
{
    struct gf_flock lock;
    int32_t error;

    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->flow.loc, loc);
    }
    else
    {
        error = ida_fd_to_loc(local, &local->flow.loc, fd);
    }
    if (unlikely(error != 0))
    {
        goto failed;
    }

    local->flow.fd = fd;
    local->flow.callback = callback;

    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
//    lock.l_start = offset;
//    lock.l_len = length;
    // FIXME: It is very bad to lock the entire file, but it is needed to avoid
    //        false conflicts while recombining iatt structures returned by
    //        potentially unordered writes.
    lock.l_start = 0;
    lock.l_len = 0;
    lock.l_owner.len = 0;
    lock.l_pid = 0;

    error = ida_nest_inodelk_common(local, IDA_EXECUTE_MAX, local->mask, ida_flow_inode_unlock_done, local->xl->name, local->flow.loc.inode, &local->flow.loc, NULL, F_SETLKW, &lock, NULL);
    if (likely(error == 0))
    {
        return;
    }

    ida_loc_unassign(&local->flow.loc);

failed:
    ida_flow_unref(local, 0, EIO, NULL);
}

void ida_flow_entry_lock_undone(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * local_next;

    local_next = local->next->local;

    ida_loc_unassign(&local_next->flow.loc);

    ida_flow_complete(local, mask, EIO, args);
}

void ida_flow_entry_lock_done(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * local_next;

    local_next = local->next->local;

    local_next->mask &= mask;

    if (unlikely(error != 0))
    {
        if (ida_bit_count(mask) < local_next->private->fragments)
        {
            local_next->flow.error = error;

            ida_flow_entry_unlock(local_next, ida_flow_entry_lock_undone, &local_next->flow.loc, local_next->flow.fd, local_next->flow.entry.name);

            return;
        }
        error = 0;
    }

    if (unlikely(args->result < 0))
    {
        ida_flow_complete(local, mask, args->code, args);
    }
    else
    {
        ida_flow_execute(local, mask, error, args);
    }

    ida_loc_unassign(&local_next->flow.loc);
}

void ida_flow_entry_lock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, const char * name)
{
    int32_t error;

    if (fd == NULL)
    {
        error = ida_loc_parent(local, &local->flow.loc, loc);
    }
    else
    {
        error = ida_fd_to_loc(local, &local->flow.loc, fd);
    }
    if (unlikely(error != 0))
    {
        goto failed;
    }

    local->flow.fd = fd;
    local->flow.callback = callback;
    local->flow.entry.name = name;

    error = ida_nest_entrylk_common(local, IDA_EXECUTE_MAX, local->mask, ida_flow_entry_lock_done, local->xl->name, local->flow.loc.inode, &local->flow.loc, NULL, name, ENTRYLK_LOCK, ENTRYLK_WRLCK, NULL);
    if (likely(error == 0))
    {
        return;
    }

    ida_loc_unassign(&local->flow.loc);

failed:
    ida_flow_complete(local, 0, error, NULL);
}

void ida_flow_entry_unlock_done(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * local_next;

    local_next = local->next->local;

    ida_loc_unassign(&local_next->flow.loc);

    ida_flow_unref(local, mask, error, args);
}

void ida_flow_entry_unlock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, const char * name)
{
    int32_t error;

    if (fd == NULL)
    {
        error = ida_loc_parent(local, &local->flow.loc, loc);
    }
    else
    {
        error = ida_fd_to_loc(local, &local->flow.loc, fd);
    }
    if (unlikely(error != 0))
    {
        goto failed;
    }

    local->flow.fd = fd;
    local->flow.callback = callback;
    local->flow.entry.name = name;
    local->error = 0;

    error = ida_nest_entrylk_common(local, IDA_EXECUTE_MAX, local->mask, ida_flow_entry_unlock_done, local->xl->name, local->flow.loc.inode, &local->flow.loc, NULL, name, ENTRYLK_UNLOCK, ENTRYLK_WRLCK, NULL);
    if (likely(error == 0))
    {
        return;
    }

    ida_loc_unassign(&local->flow.loc);

failed:
    ida_flow_unref(local, 0, EIO, NULL);
}

dict_t * ida_flow_xattr_create(ssize_t delta)
{
    dict_t * xattr;

    xattr = dict_new();
    if (unlikely(xattr == NULL))
    {
        return NULL;
    }

    if (unlikely(ida_dict_set_uint64_cow(&xattr, IDA_KEY_VERSION, 1) != 0))
    {
        dict_unref(xattr);

        return NULL;
    }

    if (unlikely(ida_dict_set_int64_cow(&xattr, IDA_KEY_SIZE, delta) != 0))
    {
        dict_unref(xattr);

        return NULL;
    }

    return xattr;
}

void ida_flow_xattr_update_undone(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * local_next;

    local_next = local->next->local;

    local_next->flow.callback = local_next->flow.xattr.callback;
    ida_flow_complete(local, mask, EIO, args);
}

void ida_flow_xattr_update_done(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * local_next;

    local_next = local->next->local;

    local_next->mask &= mask;

    if (likely(error == 0))
    {
        error = ida_dict_get_int64(args->xattrop.xattr, IDA_KEY_SIZE, &local_next->flow.xattr.size);
    }
    if (unlikely(error != 0))
    {
        if (ida_bit_count(mask) < local_next->private->fragments)
        {
            ida_flow_xattr_unlock(local_next, ida_flow_xattr_update_undone, local_next->flow.xattr.loc, local_next->flow.xattr.fd, local_next->flow.xattr.offset, local_next->flow.xattr.length, INT64_MIN);

            return;
        }
        error = 0;
    }

    local_next->flow.callback = local_next->flow.xattr.callback;
    if (unlikely(args->result < 0))
    {
        ida_flow_complete(local, mask, args->code, args);
    }
    else
    {
        ida_flow_execute(local, mask, error, args);
    }
}

void ida_flow_xattr_lock_undone(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_local_t * local_next;

    local_next = local->next->local;

    local_next->flow.callback = local_next->flow.xattr.callback;
    ida_flow_complete(local, mask, EIO, args);
}

void ida_flow_xattr_lock_done(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    dict_t * xattr;
    ida_local_t * local_next;

    local_next = local->next->local;

    local_next->mask &= mask;

    if (unlikely(error != 0))
    {
        if (ida_bit_count(mask) < local_next->private->fragments)
        {
            ida_flow_inode_unlock(local_next, ida_flow_xattr_lock_undone, local_next->flow.xattr.loc, local_next->flow.xattr.fd, local_next->flow.xattr.offset, local_next->flow.xattr.length);

            return;
        }
        error = 0;
    }

    if (unlikely(args->result < 0))
    {
        local_next->flow.callback = local_next->flow.xattr.callback;
        ida_flow_complete(local, mask, args->code, args);
    }
    else
    {
        xattr = ida_flow_xattr_create(0);
        if (unlikely(xattr == NULL))
        {
            ida_flow_inode_unlock(local_next, ida_flow_xattr_lock_undone, local_next->flow.xattr.loc, local_next->flow.xattr.fd, local_next->flow.xattr.offset, local_next->flow.xattr.length);

            return;
        }

        ida_nest_xattrop_common(local_next, IDA_EXECUTE_MAX, local_next->mask, ida_flow_xattr_update_done, local_next->inode, local_next->flow.xattr.loc, local_next->flow.xattr.fd, GF_XATTROP_ADD_ARRAY64, xattr, 0, NULL);
        dict_unref(xattr);
    }
}

void ida_flow_xattr_lock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, off_t offset, off_t length)
{
    local->flow.xattr.loc = loc;
    local->flow.xattr.fd = fd;
    local->flow.xattr.offset = offset;
    local->flow.xattr.length = length;
    local->flow.xattr.callback = callback;

    ida_flow_inode_lock(local, ida_flow_xattr_lock_done, loc, fd, offset, length);
}

void ida_flow_xattr_unlock_done(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    local = local->next->local;

    ida_flow_inode_unlock(local, local->flow.callback, local->flow.xattr.loc, local->flow.xattr.fd, local->flow.xattr.offset, local->flow.xattr.length);
}

void ida_flow_xattr_unlock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, off_t offset, off_t length, ssize_t final)
{
    dict_t * xattr;
    int32_t error;
    ssize_t delta;

    local->flow.xattr.loc = loc;
    local->flow.xattr.fd = fd;
    local->flow.xattr.offset = offset;
    local->flow.xattr.length = length;
    local->flow.callback = callback;

    if (unlikely(final == INT64_MIN))
    {
        delta = 0;
    }
    else
    {
        delta = final - local->flow.xattr.size;
    }
    xattr = ida_flow_xattr_create(delta);
    if (unlikely(xattr == NULL))
    {
        goto failed;
    }

    error = ida_nest_xattrop_common(local, IDA_EXECUTE_MAX, local->mask, ida_flow_xattr_unlock_done, local->inode, loc, fd, GF_XATTROP_ADD_ARRAY64, xattr, 0, NULL);
    dict_unref(xattr);
    if (likely(error == 0))
    {
        return;
    }

failed:
    ida_flow_unref(local, 0, EIO, NULL);
}
