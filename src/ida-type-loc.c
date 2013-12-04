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

#include <libgen.h>

#include "ida-check.h"
#include "ida-manager.h"

int32_t ida_loc_assign(ida_local_t * local, loc_t * dst, loc_t * src)
{
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, src, EINVAL);
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, dst, EINVAL);
    memset(dst, 0, sizeof(loc_t));
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, loc_copy(dst, src) == 0, EINVAL);

    return 0;
}

void ida_loc_unassign(loc_t * dst)
{
    loc_wipe(dst);
}

int32_t ida_loc_parent(ida_local_t * local, loc_t * parent, loc_t * child)
{
    char * tmp;

    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, child->parent, EINVAL);

    tmp = gf_strdup(child->path);
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, tmp, ENOMEM);
    parent->path = gf_strdup(dirname(tmp));
    GF_FREE(tmp);
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, parent->path, ENOMEM);

    parent->name = strrchr(parent->path, '/');
    if (parent->name != NULL)
    {
        parent->name++;
    }

    parent->inode  = inode_ref(child->parent);
    if (unlikely(parent->inode == NULL))
    {
        goto failed;
    }
    if (!uuid_is_null(child->pargfid))
    {
        uuid_copy(parent->gfid, child->pargfid);
    }

    parent->parent = inode_parent(parent->inode, 0, NULL);
    if (parent->parent != NULL)
    {
        if (!uuid_is_null(parent->parent->gfid))
        {
            uuid_copy(parent->pargfid, parent->parent->gfid);
        }
    }

    return 0;

failed:
    GF_FREE((char *)parent->path);

    return ENOMEM;
}
