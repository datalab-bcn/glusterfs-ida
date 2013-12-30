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

int32_t ida_str_assign(ida_local_t * local, char ** dst, const char * src)
{
    SYS_PTR(
        dst, gf_strdup, (src),
        EINVAL,
        E(),
        RETERR()
    );

    return 0;
}

void ida_str_unassign(char ** dst)
{
    if (*dst != NULL)
    {
        GF_FREE(*dst);
        *dst = NULL;
    }
}

int32_t ida_str_combine(ida_local_t * local, const char * dst, const char * src)
{
    if (strcmp(dst, src) != 0)
    {
        return EIO;
    }

    return 0;
}
