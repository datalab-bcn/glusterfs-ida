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

#ifndef __IDA_TYPES_H__
#define __IDA_TYPES_H__

#include "xlator.h"

typedef struct _ida_local ida_local_t;
typedef union _ida_args ida_args_t;
typedef struct _ida_args_cbk ida_args_cbk_t;

typedef struct _ida_heal
{
    uuid_t gfid;
    loc_t loc;
    int32_t refs;
    int32_t error;
    int32_t healing;
    mode_t mode;
    inode_t * inode;
    fd_t * src;
    fd_t * dst;
    uint64_t offset;
    uint32_t size;
    uintptr_t src_mask;
    uintptr_t dst_mask;
} ida_heal_t;

typedef struct
{
    struct iobref * buffers;
    struct iovec * vectors;
    uint32_t count;
} ida_buffer_t;

typedef struct _ida_dirent_node
{
    struct _ida_dirent_node * childs[256];
    struct _ida_dirent_node * parent;
    struct _ida_dirent_node * siblings;
    uint8_t * key;
    uint32_t length;
    uint32_t count;
    gf_dirent_t * entry;
} ida_dirent_node_t;

typedef struct
{
    ida_dirent_node_t * childs[256];
    gf_dirent_t entries;
    uint32_t count;
} ida_dirent_t;

typedef struct
{
    ida_dirent_t entries;
    gf_dirent_t * current;
    off_t current_offset;
    int32_t cached;
} ida_dir_ctx_t;

typedef struct
{
    uint64_t size;
    ida_heal_t heal;
} ida_inode_ctx_t;

typedef void (* ida_manager_wipe_f)(ida_local_t * local);
typedef int32_t (* ida_manager_dispatch_f)(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args);
typedef int32_t (* ida_manager_combine_f)(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src);
typedef int32_t (* ida_manager_rebuild_f)(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args);
typedef void (* ida_manager_finish_f)(ida_local_t * local);

typedef void (* ida_callback_f)(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args);

#endif /* __IDA_TYPES_H__ */
