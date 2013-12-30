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

#include "ida-manager.h"

int32_t ida_inode_assign(ida_local_t * local, inode_t ** dst, inode_t * src)
{
    *dst = inode_ref(src);

    return 0;
}

void ida_inode_unassign(inode_t ** dst)
{
    if (*dst != NULL)
    {
        inode_unref(*dst);
        *dst = NULL;
    }
}

int32_t ida_inode_combine(ida_local_t * local, inode_t * dst, inode_t * src)
{
    if ((dst == src) || (uuid_compare(dst->gfid, src->gfid) == 0))
    {
        return 0;
    }

    return EIO;
}
