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

#ifndef __IDA_DIRENT_H__
#define __IDA_DIRENT_H__

#include "xlator.h"

#include "ida-types.h"

gf_dirent_t * ida_dirent_dup(ida_local_t * local, gf_dirent_t * dirent);
int32_t ida_dirent_assign(ida_local_t * local, ida_dirent_t * dst, gf_dirent_t * src);
void ida_dirent_unassign(ida_dirent_t * dst);
bool ida_dirent_combine(ida_dirent_t * dst, ida_dirent_t * src);
void ida_dirent_clean(ida_local_t * local, ida_dirent_t * dst, int32_t cut);

#endif /* __IDA_DIRENT_H__ */
