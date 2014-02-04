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

#ifndef __IDA_COMBINE_H__
#define __IDA_COMBINE_H__

bool ida_prepare_access(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_create(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_entrylk(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_fentrylk(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_flush(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_fsync(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_fsyncdir(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_getxattr(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_fgetxattr(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_inodelk(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_finodelk(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_link(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_lk(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_lookup(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_mkdir(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_mknod(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_open(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_opendir(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_rchecksum(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_readdir(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_readdirp(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_readlink(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_readv(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_removexattr(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_fremovexattr(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_rename(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_rmdir(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_setattr(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_fsetattr(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_setxattr(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_fsetxattr(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_stat(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_fstat(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_statfs(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_symlink(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_truncate(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_ftruncate(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_unlink(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_writev(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_xattrop(ida_private_t * ida, ida_request_t * req);
bool ida_prepare_fxattrop(ida_private_t * ida, ida_request_t * req);

bool ida_combine_access(ida_request_t * req, uint32_t idx,
                        ida_answer_t * ans, uintptr_t * data);
bool ida_combine_create(ida_request_t * req, uint32_t idx,
                        ida_answer_t * ans, uintptr_t * data);
bool ida_combine_entrylk(ida_request_t * req, uint32_t idx,
                         ida_answer_t * ans, uintptr_t * data);
bool ida_combine_fentrylk(ida_request_t * req, uint32_t idx,
                          ida_answer_t * ans, uintptr_t * data);
bool ida_combine_flush(ida_request_t * req, uint32_t ida,
                       ida_answer_t * ans, uintptr_t * data);
bool ida_combine_fsync(ida_request_t * req, uint32_t ida,
                       ida_answer_t * ans, uintptr_t * data);
bool ida_combine_fsyncdir(ida_request_t * req, uint32_t ida,
                          ida_answer_t * ans, uintptr_t * data);
bool ida_combine_getxattr(ida_request_t * req, uint32_t ida,
                          ida_answer_t * ans, uintptr_t * data);
bool ida_combine_fgetxattr(ida_request_t * req, uint32_t ida,
                           ida_answer_t * ans, uintptr_t * data);
bool ida_combine_inodelk(ida_request_t * req, uint32_t ida,
                         ida_answer_t * ans, uintptr_t * data);
bool ida_combine_finodelk(ida_request_t * req, uint32_t ida,
                          ida_answer_t * ans, uintptr_t * data);
bool ida_combine_link(ida_request_t * req, uint32_t ida,
                      ida_answer_t * ans, uintptr_t * data);
bool ida_combine_lk(ida_request_t * req, uint32_t ida,
                    ida_answer_t * ans, uintptr_t * data);
bool ida_combine_lookup(ida_request_t * req, uint32_t ida,
                        ida_answer_t * ans, uintptr_t * data);
bool ida_combine_mkdir(ida_request_t * req, uint32_t ida,
                       ida_answer_t * ans, uintptr_t * data);
bool ida_combine_mknod(ida_request_t * req, uint32_t ida,
                       ida_answer_t * ans, uintptr_t * data);
bool ida_combine_open(ida_request_t * req, uint32_t ida,
                      ida_answer_t * ans, uintptr_t * data);
bool ida_combine_opendir(ida_request_t * req, uint32_t ida,
                         ida_answer_t * ans, uintptr_t * data);
bool ida_combine_rchecksum(ida_request_t * req, uint32_t ida,
                           ida_answer_t * ans, uintptr_t * data);
bool ida_combine_readdir(ida_request_t * req, uint32_t ida,
                         ida_answer_t * ans, uintptr_t * data);
bool ida_combine_readdirp(ida_request_t * req, uint32_t ida,
                          ida_answer_t * ans, uintptr_t * data);
bool ida_combine_readlink(ida_request_t * req, uint32_t ida,
                          ida_answer_t * ans, uintptr_t * data);
bool ida_combine_readv(ida_request_t * req, uint32_t ida,
                       ida_answer_t * ans, uintptr_t * data);
bool ida_combine_removexattr(ida_request_t * req, uint32_t ida,
                             ida_answer_t * ans, uintptr_t * data);
bool ida_combine_fremovexattr(ida_request_t * req, uint32_t ida,
                              ida_answer_t * ans, uintptr_t * data);
bool ida_combine_rename(ida_request_t * req, uint32_t ida,
                        ida_answer_t * ans, uintptr_t * data);
bool ida_combine_rmdir(ida_request_t * req, uint32_t ida,
                       ida_answer_t * ans, uintptr_t * data);
bool ida_combine_setattr(ida_request_t * req, uint32_t ida,
                         ida_answer_t * ans, uintptr_t * data);
bool ida_combine_fsetattr(ida_request_t * req, uint32_t ida,
                          ida_answer_t * ans, uintptr_t * data);
bool ida_combine_setxattr(ida_request_t * req, uint32_t ida,
                          ida_answer_t * ans, uintptr_t * data);
bool ida_combine_fsetxattr(ida_request_t * req, uint32_t ida,
                           ida_answer_t * ans, uintptr_t * data);
bool ida_combine_stat(ida_request_t * req, uint32_t ida,
                      ida_answer_t * ans, uintptr_t * data);
bool ida_combine_fstat(ida_request_t * req, uint32_t ida,
                       ida_answer_t * ans, uintptr_t * data);
bool ida_combine_statfs(ida_request_t * req, uint32_t ida,
                        ida_answer_t * ans, uintptr_t * data);
bool ida_combine_symlink(ida_request_t * req, uint32_t ida,
                         ida_answer_t * ans, uintptr_t * data);
bool ida_combine_truncate(ida_request_t * req, uint32_t ida,
                          ida_answer_t * ans, uintptr_t * data);
bool ida_combine_ftruncate(ida_request_t * req, uint32_t ida,
                           ida_answer_t * ans, uintptr_t * data);
bool ida_combine_unlink(ida_request_t * req, uint32_t ida,
                        ida_answer_t * ans, uintptr_t * data);
bool ida_combine_writev(ida_request_t * req, uint32_t ida,
                        ida_answer_t * ans, uintptr_t * data);
bool ida_combine_xattrop(ida_request_t * req, uint32_t ida,
                         ida_answer_t * ans, uintptr_t * data);
bool ida_combine_fxattrop(ida_request_t * req, uint32_t ida,
                          ida_answer_t * ans, uintptr_t * data);

int32_t ida_rebuild_access(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * data);
int32_t ida_rebuild_create(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * data);
int32_t ida_rebuild_entrylk(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * data);
int32_t ida_rebuild_fentrylk(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);
int32_t ida_rebuild_flush(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * data);
int32_t ida_rebuild_fsync(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * data);
int32_t ida_rebuild_fsyncdir(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);
int32_t ida_rebuild_getxattr(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);
int32_t ida_rebuild_fgetxattr(ida_private_t * ida, ida_request_t * req,
                              ida_answer_t * data);
int32_t ida_rebuild_inodelk(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * data);
int32_t ida_rebuild_finodelk(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);
int32_t ida_rebuild_link(ida_private_t * ida, ida_request_t * req,
                         ida_answer_t * data);
int32_t ida_rebuild_lk(ida_private_t * ida, ida_request_t * req,
                       ida_answer_t * data);
int32_t ida_rebuild_lookup(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * data);
int32_t ida_rebuild_mkdir(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * data);
int32_t ida_rebuild_mknod(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * data);
int32_t ida_rebuild_open(ida_private_t * ida, ida_request_t * req,
                         ida_answer_t * data);
int32_t ida_rebuild_opendir(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * data);
int32_t ida_rebuild_rchecksum(ida_private_t * ida, ida_request_t * req,
                              ida_answer_t * data);
int32_t ida_rebuild_readdir(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * data);
int32_t ida_rebuild_readdirp(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);
int32_t ida_rebuild_readlink(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);
int32_t ida_rebuild_readv(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * data);
int32_t ida_rebuild_removexattr(ida_private_t * ida, ida_request_t * req,
                                ida_answer_t * data);
int32_t ida_rebuild_fremovexattr(ida_private_t * ida, ida_request_t * req,
                                 ida_answer_t * data);
int32_t ida_rebuild_rename(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * data);
int32_t ida_rebuild_rmdir(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * data);
int32_t ida_rebuild_setattr(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * data);
int32_t ida_rebuild_fsetattr(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);
int32_t ida_rebuild_setxattr(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);
int32_t ida_rebuild_fsetxattr(ida_private_t * ida, ida_request_t * req,
                              ida_answer_t * data);
int32_t ida_rebuild_stat(ida_private_t * ida, ida_request_t * req,
                         ida_answer_t * data);
int32_t ida_rebuild_fstat(ida_private_t * ida, ida_request_t * req,
                          ida_answer_t * data);
int32_t ida_rebuild_statfs(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * data);
int32_t ida_rebuild_symlink(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * data);
int32_t ida_rebuild_truncate(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);
int32_t ida_rebuild_ftruncate(ida_private_t * ida, ida_request_t * req,
                              ida_answer_t * data);
int32_t ida_rebuild_unlink(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * data);
int32_t ida_rebuild_writev(ida_private_t * ida, ida_request_t * req,
                           ida_answer_t * data);
int32_t ida_rebuild_xattrop(ida_private_t * ida, ida_request_t * req,
                            ida_answer_t * data);
int32_t ida_rebuild_fxattrop(ida_private_t * ida, ida_request_t * req,
                             ida_answer_t * data);

#endif /* __IDA_COMBINE_H__ */
