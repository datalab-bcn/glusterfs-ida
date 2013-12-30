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

#include "ida.h"

//static uint32_t dict_count = 0;

int32_t ida_dict_special(char * key)
{
    return /*(strcmp(key, IDA_KEY_VERSION) == 0) ||*/
           (strcmp(key, GF_CONTENT_KEY) == 0);
}

int32_t ida_dict_data_compare(data_t * dst, data_t * src)
{
    if (dst->len < src->len)
    {
        return -1;
    }
    if (dst->len > src->len)
    {
        return 1;
    }
    return memcmp(dst->data, src->data, dst->len);
}

int ida_dict_combine_enum(dict_t * src, char * key, data_t * value, void * arg)
{
//    dict_t * new;
    dict_t ** dict;
    data_t * tmp;

    dict = arg;

    tmp = dict_get(*dict, key);
    if (tmp != NULL)
    {
        if (ida_dict_special(key))
        {
            if (value->len != tmp->len)
            {
                return -1;
            }
        }
        else if (ida_dict_data_compare(value, tmp) != 0)
        {
            return -1;
        }
    }
    else
    {
        return -1;
/*
        if (ida_dict_special(key))
        {
            goto failed;
        }

        if (data->dict->refcount != 1)
        {
            new = dict_copy(data->dict, NULL);
            if (unlikely(new == NULL))
            {
                goto failed;
            }
            dict_unref(data->dict);
            dict_ref(new);
            data->dict = new;
        }

        if (unlikely(dict_set(data->dict, key, value) != 0))
        {
            goto failed;
        }
*/
    }

    return 0;
}

int32_t ida_dict_combine_cow(ida_local_t * local, dict_t ** dst, dict_t * src)
{
    if (src == NULL)
    {
        return 0;
    }

    return dict_foreach(src, ida_dict_combine_enum, dst);
}

int ida_dict_compare_enum(dict_t * src, char * key, data_t * value, void * arg)
{
//    dict_t * new;
    dict_t * dict;
    data_t * tmp;

    dict = arg;

    tmp = dict_get(dict, key);
    if (tmp != NULL)
    {
        if (ida_dict_special(key))
        {
            if (value->len != tmp->len)
            {
                return -1;
            }
        }
        else if (ida_dict_data_compare(value, tmp) != 0)
        {
            return -1;
        }
    }
    else
    {
        return -1;
    }

    return 0;
}

bool ida_dict_compare(dict_t * dst, dict_t * src)
{
    if ((dst == NULL) || (src == NULL))
    {
        return dst == src;
    }
    if (dst->count != src->count)
    {
        logW("dict-compare: mismatching number of items");
        return false;
    }
    return dict_foreach(src, ida_dict_compare_enum, dst) == 0;
}
