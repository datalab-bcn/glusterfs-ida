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

#include "byte-order.h"

#include "ida-check.h"
#include "ida-memory.h"
#include "ida.h"

struct ida_dict_data
{
    dict_t * dict;
    int32_t equal;
};

//static uint32_t dict_count = 0;

int32_t ida_dict_assign(ida_local_t * local, dict_t ** dst, dict_t * src)
{
    if (src != NULL)
    {
        *dst = dict_ref(src);

//        gf_log_callingfn("dict", GF_LOG_DEBUG, "dict assign [%5u] %p (%u)", __sync_add_and_fetch(&dict_count, 1), *dst, (*dst)->refcount);
        IDA_VALIDATE_OR_RETURN_ERROR("dict", *dst, EINVAL);
    }
    else
    {
        *dst = NULL;
    }

    return 0;
}

int32_t ida_dict_new(ida_local_t * local, dict_t ** dst, dict_t * src)
{
    if (src != NULL)
    {
        *dst = dict_ref(src);
//        gf_log_callingfn("dict", GF_LOG_DEBUG, "dict assign [%5u] %p (%u)", __sync_add_and_fetch(&dict_count, 1), *dst, (*dst)->refcount);
        IDA_VALIDATE_OR_RETURN_ERROR("dict", *dst, EINVAL);
    }
    else
    {
        *dst = dict_new();
//        gf_log_callingfn("dict", GF_LOG_DEBUG, "dict assign [%5u] %p (%u)", __sync_add_and_fetch(&dict_count, 1), *dst, (*dst)->refcount);
        IDA_VALIDATE_OR_RETURN_ERROR("dict", *dst, ENOMEM);
    }

    return 0;
}

void ida_dict_unassign(dict_t ** dst)
{
    if (*dst != NULL)
    {
//        gf_log_callingfn("dict", GF_LOG_DEBUG, "dict unassign [%5u] %p (%u)", __sync_sub_and_fetch(&dict_count, 1), *dst, (*dst)->refcount);
        dict_unref(*dst);
        *dst = NULL;
    }
}

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

static void ida_dict_equal_enum(dict_t * dst, char * key, data_t * value, void * arg)
{
    struct ida_dict_data * data;
    data_t * tmp;

    data = arg;

    if (unlikely(data->equal == 0))
    {
        return;
    }

    tmp = dict_get(data->dict, key);
    if (tmp == NULL)
    {
        data->equal = 0;
    }
    else
    {
        if (!ida_dict_special(key) && (ida_dict_data_compare(value, tmp) != 0))
        {
            data->equal = 0;
        }
    }
}

int32_t ida_dict_equal(dict_t * dst, dict_t * src)
{
    struct ida_dict_data data;

    if (dst == src)
    {
        return 1;
    }

    if (dst->count != src->count)
    {
        return 0;
    }

    data.dict = src;
    data.equal = 1;

    dict_foreach(dst, ida_dict_equal_enum, &data);

    return data.equal;
}

int32_t ida_dict_set_cow(dict_t ** dst, char * key, data_t * value)
{
    dict_t * new;
    data_t * tmp;

    if ((*dst)->refcount != 1)
    {
        tmp = dict_get(*dst, key);
        if ((tmp == NULL) || (ida_dict_data_compare(value, tmp) != 0))
        {
            new = dict_copy(*dst, NULL);
            if (unlikely(new == NULL))
            {
                return -1;
            }
            ida_dict_unassign(dst);
            ida_dict_assign(NULL, dst, new);
        }
    }

    return dict_set(*dst, key, value);
}

int32_t ida_dict_set_bin_cow(dict_t ** dst, char * key, void * value, uint32_t length)
{
    dict_t * new;
    data_t * tmp;

    if ((*dst)->refcount != 1)
    {
        tmp = dict_get(*dst, key);
        if ((tmp == NULL) || (ida_dict_data_compare(value, tmp) != 0))
        {
            new = dict_copy(*dst, NULL);
            if (unlikely(new == NULL))
            {
                return -1;
            }
            ida_dict_unassign(dst);
            ida_dict_assign(NULL, dst, new);
        }
    }

    return dict_set_bin(*dst, key, value, length);
}

int32_t ida_dict_get_bin(dict_t * src, char * key, void * value, uint32_t * length)
{
    data_t * data;
    int32_t error, size;

    error = ENOENT;
    data = dict_get(src, key);
    if (data != NULL)
    {
        error = ENOBUFS;
        size = *length;
        *length = data->len;
        if (size >= data->len)
        {
            error = 0;
            size = data->len;
        }
        memcpy(value, data->data, size);
    }

    return error;
}

#define hton8(_x) _x
#define ntoh8(_x) _x

#define IDA_DICT_SET_COW(_type, _size) \
    int32_t ida_dict_set_##_type##_size##_cow(dict_t ** dst, char * key, _type##_size##_t value) \
    { \
        int32_t error; \
        typeof(value) * ptr = IDA_CALLOC_OR_RETURN_ERROR("ida-dict", sizeof(value), uint8_t, ENOMEM); \
        *ptr = hton##_size(value); \
        error = ida_dict_set_bin_cow(dst, key, ptr, sizeof(value)); \
        if (unlikely(error != 0)) \
        { \
            GF_FREE(ptr); \
        } \
        return error; \
    }

#define IDA_DICT_GET(_type, _size) \
    int32_t ida_dict_get_##_type##_size(dict_t * src, char * key, _type##_size##_t * value) \
    { \
        typeof(*value) tmp; \
        uint32_t size = sizeof(tmp); \
        int32_t error = ida_dict_get_bin(src, key, &tmp, &size); \
        if (likely(error == 0)) \
        { \
            *value = ntoh##_size(tmp); \
        } \
        return error; \
    }

IDA_DICT_SET_COW(int, 8)
IDA_DICT_SET_COW(int, 16)
IDA_DICT_SET_COW(int, 32)
IDA_DICT_SET_COW(int, 64)
IDA_DICT_SET_COW(uint, 16)
IDA_DICT_SET_COW(uint, 32)
IDA_DICT_SET_COW(uint, 64)

IDA_DICT_GET(int, 8)
IDA_DICT_GET(int, 16)
IDA_DICT_GET(int, 32)
IDA_DICT_GET(int, 64)
IDA_DICT_GET(uint, 16)
IDA_DICT_GET(uint, 32)
IDA_DICT_GET(uint, 64)

int32_t ida_dict_del_cow(dict_t ** dst, char * key)
{
    dict_t * new;

    if ((*dst)->refcount != 1)
    {
        if (dict_get(*dst, key) != NULL)
        {
            new = dict_copy(*dst, NULL);
            if (unlikely(new == NULL))
            {
                return -1;
            }
            dict_unref(*dst);
            dict_ref(new);
            *dst = new;
        }
    }

    dict_del(*dst, key);

    return 0;
}

void ida_dict_clean_enum(dict_t * src, char * key, data_t * value, void * arg)
{
    struct ida_dict_data * data;

    data = arg;

    if (unlikely(data->equal == 0))
    {
        return;
    }

    if (ida_dict_special(key))
    {
        if (ida_dict_del_cow(&data->dict, key) != 0)
        {
            data->equal = 0;
        }
    }
}

int32_t ida_dict_clean_cow(dict_t ** dst)
{
    struct ida_dict_data data;

    data.dict = *dst;
    data.equal = 1;

    dict_foreach(*dst, ida_dict_clean_enum, &data);
    *dst = data.dict;

    return data.equal - 1;
}

void ida_dict_combine_enum(dict_t * src, char * key, data_t * value, void * arg)
{
    struct ida_dict_data * data;
//    dict_t * new;
    data_t * tmp;

    data = arg;

    if (unlikely(data->equal == 0))
    {
        return;
    }

    tmp = dict_get(data->dict, key);
    if (tmp != NULL)
    {
        if (ida_dict_special(key))
        {
            if (value->len != tmp->len)
            {
                goto failed;
            }
        }
        else if (ida_dict_data_compare(value, tmp) != 0)
        {
            goto failed;
        }
    }
    else
    {
        goto failed;
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

    return;

failed:
    data->equal = 0;
}

int32_t ida_dict_combine_cow(ida_local_t * local, dict_t ** dst, dict_t * src)
{
    struct ida_dict_data data;

    if (src == NULL)
    {
        return 0;
    }

    data.dict = *dst;
    data.equal = 1;

    dict_foreach(src, ida_dict_combine_enum, &data);
    *dst = data.dict;

    return data.equal - 1;
}
