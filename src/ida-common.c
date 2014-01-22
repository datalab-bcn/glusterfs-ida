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

#include "ida-common.h"
#include "ida.h"

struct ida_xattr_merge_data
{
    ida_local_t * local;
    dict_t ** dict;
};

off_t ida_offset_adjust(ida_local_t * local, off_t offset)
{
    return offset - (offset % local->private->block_size);
}

size_t ida_size_adjust(ida_local_t * local, size_t size)
{
    size += local->private->block_size - ((size + local->private->block_size - 1) % local->private->block_size) - 1;

    return size / local->private->fragments;
}

void ida_offset_size_adjust(ida_local_t * local, off_t * offset, size_t * size)
{
    off_t delta;
    size_t extra;

    delta = *offset % local->private->block_size;
    (*offset) -= delta;

    if (*size > 0)
    {
        extra = local->private->block_size - ((*size + delta - 1) % local->private->block_size) - 1;
        (*size) += extra;
    }
}

int32_t ida_xattr_update_content(ida_local_t * local, dict_t * dict, size_t * old)
{
    uint64_t content;
    size_t size;
    int32_t res;

    *old = 0;
    res = dict_get_uint64(dict, GF_CONTENT_KEY, &content);
    if (res != 0)
    {
        if (res == -ENOENT)
        {
            res = 0;
        }
        return -res;
    }

    size = ida_size_adjust(local, content);

    res = dict_set_uint64(dict, GF_CONTENT_KEY, size);
    if (res == 0)
    {
        *old = content;
    }

    return -res;
}

int32_t ida_xattr_copy(ida_local_t * local, dict_t ** dst, dict_t * src)
{
    *dst = dict_ref(src);

    return (*dst == NULL) ? EIO : 0;
}

int ida_xattr_merge_add(dict_t * src, char * key, data_t * value, void * arg)
{
    struct ida_xattr_merge_data * data;
    data_t * tmp;
    dict_t * dict;

    data = arg;

    dict = *data->dict;

    tmp = dict_get(dict, key);
    if (tmp != NULL)
    {
        if (strcmp(key, GF_CONTENT_KEY) != 0)
        {
            if ((value->len != tmp->len) || (memcmp(value->data, tmp->data, value->len) != 0))
            {
                gf_log(data->local->xl->name, GF_LOG_WARNING, "key '%s' is different", key);

                return -1;
            }
        }
    }
    else
    {
        if (dict->refcount != 1)
        {
            dict = dict_copy(dict, NULL);
            dict_unref(*data->dict);
            *data->dict = dict;
            if (unlikely(dict == NULL))
            {
                return -1;
            }
        }

        if (dict_set(dict, key, value) != 0)
        {
            return -1;
        }
    }

    return 0;
}

int32_t ida_xattr_merge(ida_local_t * local, dict_t ** dst, dict_t * src)
{
    struct ida_xattr_merge_data data;

    data.local = local;
    data.dict = dst;

    return (dict_foreach(src, ida_xattr_merge_add, &data) < 0) ? EIO : 0;
}
