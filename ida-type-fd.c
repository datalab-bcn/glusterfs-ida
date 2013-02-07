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
#include "ida-manager.h"
#include "ida-type-inode.h"

int32_t ida_fd_assign(ida_local_t * local, fd_t ** dst, fd_t * src)
{
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, src, EINVAL);
    *dst = fd_ref(src);
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, *dst, EINVAL);

    return 0;
}

void ida_fd_unassign(fd_t ** dst)
{
    if (*dst != NULL)
    {
        fd_unref(*dst);
        *dst = NULL;
    }
}

int32_t ida_fd_combine(ida_local_t * local, fd_t * dst, fd_t * src)
{
    if ((dst == src) ||
        ((dst->pid == src->pid) && (ida_inode_combine(local, dst->inode, src->inode) == 0)))
    {
        return 0;
    }

    return EIO;
}

int32_t ida_fd_to_loc(ida_local_t * local, loc_t * loc, fd_t * fd)
{
    int32_t error;

    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, fd->inode, EINVAL);

    error = inode_path(fd->inode, NULL, (char **)&loc->path);
    if (unlikely(error < 0))
    {
        return -error;
    }

    loc->name = strrchr(loc->path, '/');
    if (loc->name != NULL)
    {
        loc->name++;
    }

    error = ida_inode_assign(local, &loc->inode, fd->inode);
    if (unlikely(error != 0))
    {
        goto failed;
    }
    if (!uuid_is_null(loc->inode->gfid))
    {
        uuid_copy(loc->gfid, loc->inode->gfid);
    }

    loc->parent = inode_parent(loc->inode, 0, NULL);
    if (loc->parent != NULL)
    {
        if (!uuid_is_null(loc->parent->gfid))
        {
            uuid_copy(loc->pargfid, loc->parent->gfid);
        }
    }

    return 0;

failed:
    GF_FREE((char *)loc->path);

    return ENOMEM;
}
