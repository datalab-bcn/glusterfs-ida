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

#ifndef __IDA_MEM_TYPES_H__
#define __IDA_MEM_TYPES_H__

#include "mem-types.h"

enum gf_ida_mem_types_
{
    ida_mt_ida_private_t = sys_mt_end + 1,
    ida_mt_ida_local_t,
    ida_mt_ida_dirent_node_t,
    ida_mt_ida_dir_ctx_t,
    ida_mt_ida_inode_ctx_t,
    ida_mt_ida_heal_t,
    ida_mt_xlator_t,
    ida_mt_ida_fd_ctx_t,
    ida_mt_uint8_t,
    ida_mt_end
};

#endif /* __IDA_MEM_TYPES_H__ */
