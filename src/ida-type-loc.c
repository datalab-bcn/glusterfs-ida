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

#include "gfsys.h"

#include <libgen.h>

#include "ida-manager.h"

int32_t ida_loc_assign(ida_local_t * local, loc_t * dst, loc_t * src)
{
    memset(dst, 0, sizeof(loc_t));
    SYS_CODE(
        loc_copy, (dst, src),
        EINVAL,
        E(),
        RETERR()
    );

    return 0;
}

void ida_loc_unassign(loc_t * dst)
{
    loc_wipe(dst);
}

int32_t ida_loc_parent(ida_local_t * local, loc_t * parent, loc_t * child)
{
    char * tmp;

    SYS_PTR(
        &tmp, gf_strdup, (child->path),
        ENOMEM,
        E(),
        RETERR()
    );
    SYS_PTR(
        &parent->path, gf_strdup, (dirname(tmp)),
        ENOMEM,
        E(),
        GOTO(failed)
    );

    parent->name = strrchr(parent->path, '/');
    if (parent->name != NULL)
    {
        parent->name++;
    }

    SYS_PTR(
        &parent->inode, inode_ref, (child->parent),
        ENOMEM,
        E(),
        GOTO(failed_parent)
    );
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

    SYS_FREE(tmp);

    return 0;

failed_parent:
    SYS_FREE((char *)parent->path);
failed:
    SYS_FREE(tmp);

    return ENOMEM;
}
