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

#ifndef __IDA_INODE_H__
#define __IDA_INODE_H__

#include "xlator.h"

#include "ida-types.h"

int32_t ida_inode_assign(ida_local_t * local, inode_t ** dst, inode_t * src);
void ida_inode_unassign(inode_t ** dst);
int32_t ida_inode_combine(ida_local_t * local, inode_t * dst, inode_t * src);

#endif /* __IDA_INODE_H__ */
