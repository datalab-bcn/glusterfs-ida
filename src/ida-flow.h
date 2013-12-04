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

#ifndef __IDA_FLOW_H__
#define __IDA_FLOW_H__


#include "ida-check.h"
#include "ida-common.h"
#include "ida-manager.h"
#include "ida.h"

void ida_flow_inode_lock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, off_t offset, off_t length);
void ida_flow_inode_unlock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, off_t offset, off_t length);

void ida_flow_entry_lock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, const char * name);
void ida_flow_entry_unlock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, const char * name);

void ida_flow_xattr_lock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, off_t offset, off_t length);
void ida_flow_xattr_unlock(ida_local_t * local, ida_callback_f callback, loc_t * loc, fd_t * fd, off_t offset, off_t length, ssize_t final);

#endif /* __IDA_FLOW_H__ */
