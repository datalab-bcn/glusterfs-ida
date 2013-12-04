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

#include "ida-check.h"
#include "ida-memory.h"
#include "ida-type-iatt.h"
#include "ida-type-inode.h"
#include "ida-type-dict.h"
#include "ida-manager.h"

void ida_dirent_wipe(gf_dirent_t * dirent)
{
    ida_iatt_unassign(&dirent->d_stat);
    ida_inode_unassign(&dirent->inode);
    ida_dict_unassign(&dirent->dict);

    GF_FREE(dirent);
}

gf_dirent_t * ida_dirent_dup(ida_local_t * local, gf_dirent_t * dirent)
{
    gf_dirent_t * copy;
    int32_t error, len;

    len = strlen(dirent->d_name);
    if (len != dirent->d_len)
    {
        gf_log(local->xl->name, GF_LOG_WARNING, "Invalid d_len in dirent");
    }

    copy = gf_dirent_for_name(dirent->d_name);
    if (unlikely(copy == NULL))
    {
        return NULL;
    }
    memset(copy, 0, sizeof(gf_dirent_t));

    INIT_LIST_HEAD(&copy->list);
    copy->d_ino = dirent->d_ino;
    copy->d_off = dirent->d_off;
    copy->d_len = len;
    copy->d_type = dirent->d_type;

    error = ida_iatt_assign(local, &copy->d_stat, &dirent->d_stat);
    if (likely(error == 0))
    {
        if (dirent->inode != NULL)
        {
            error = ida_inode_assign(local, &copy->inode, dirent->inode);
        }
    }
    if (likely(error == 0))
    {
        if (dirent->dict != NULL)
        {
            error = ida_dict_assign(local, &copy->dict, dirent->dict);
        }
    }
    if (unlikely(error != 0))
    {
        ida_dirent_wipe(copy);

        return NULL;
    }

    return copy;
}

ida_dirent_node_t * ida_dirent_new(ida_local_t * local, gf_dirent_t * dirent, uint8_t * key, int32_t length)
{
    gf_dirent_t * copy;
    ida_dirent_node_t * node;

    node = IDA_ALLOC_OR_RETURN_NULL(local->xl->name, sizeof(ida_dirent_node_t), gf_ida_mt_ida_dirent_node_t);

    memset(&node->childs, 0, sizeof(node->childs));
    node->parent = NULL;
    node->siblings = NULL;
    node->count = 1;

    if (dirent != NULL)
    {
        copy = ida_dirent_dup(local, dirent);
        if (unlikely(copy == NULL))
        {
            GF_FREE(node);

            return NULL;
        }

        node->key = (uint8_t *)copy->d_name;
        node->length = strlen(copy->d_name);
        node->entry = copy;
    }
    else
    {
        node->key = key;
        node->length = length;
        node->entry = NULL;
    }

    return node;
}

void ida_dirent_free(ida_dirent_node_t * node)
{
    ida_dirent_node_t * sibling;

    do
    {
        sibling = node;
        node = sibling->siblings;
        if ((sibling->entry != NULL) && list_empty(&sibling->entry->list))
        {
            ida_dirent_wipe(sibling->entry);
        }
        GF_FREE(sibling);
    } while (node != NULL);
}

int32_t ida_dirent_split(ida_local_t * local, ida_dirent_node_t ** root, ida_dirent_node_t * node, int32_t length)
{
    ida_dirent_node_t * new;

    if (node->length > length)
    {
        node->key += length;
        node->length -= length;

        new = ida_dirent_new(local, NULL, (*root)->key, length);
        if (unlikely(new == NULL))
        {
            return -1;
        }

        (*root)->key += length;
        (*root)->length -= length;

        new->childs[*node->key] = node;
        new->childs[*(*root)->key] = *root;

        new->parent = (*root)->parent;
        (*root)->parent = new;
        node->parent = new;

        *root = new;
    }
    else
    {
        (*root)->key += length;
        (*root)->length -= length;

        node->childs[*(*root)->key] = *root;

        node->parent = (*root)->parent;
        (*root)->parent = node;

        *root = node;
    }

    return 0;
}

int32_t ida_dirent_merge_combine(ida_local_t * local, gf_dirent_t * dst, gf_dirent_t * src)
{
    int32_t error;

    error = 0;
    if (unlikely(dst->d_type != src->d_type) ||
        unlikely(dst->d_len != src->d_len) /*||
        unlikely(dst->d_ino != src->d_ino)*/)
    {
        error = EIO;
    }
    if (likely(error == 0))
    {
        if (dst->inode == NULL)
        {
            error = (src->inode != NULL) ? EIO : 0;
        }
        else if (src->inode == NULL)
        {
            error = EIO;
        }
        else
        {
            error = ida_inode_combine(local, dst->inode, src->inode);
        }
    }
    if (likely(error == 0))
    {
        error = ida_iatt_combine(local, &dst->d_stat, &src->d_stat);
    }
    if (likely(error == 0))
    {
        if (dst->dict == NULL)
        {
            error = (src->dict != NULL) ? EIO : 0;
        }
        else if (src->dict == NULL)
        {
            error = EIO;
        }
        else
        {
            error = ida_dict_combine_cow(local, &dst->dict, src->dict);
        }
    }

    return error;
}

void ida_dirent_merge(ida_local_t * local, ida_dirent_node_t * base, ida_dirent_node_t * node)
{
    ida_dirent_node_t * sibling;

    sibling = base;
    do
    {
        if (ida_dirent_merge_combine(local, sibling->entry, node->entry) == 0)
        {
            sibling->count++;

            ida_dirent_free(node);

            return;
        }

        sibling = sibling->siblings;
    } while (sibling != NULL);

    node->siblings = base->siblings;
    base->siblings = node;

    list_add(&node->entry->list, &base->entry->list);
}

int32_t ida_dirent_insert(ida_local_t * local, ida_dirent_node_t ** root, ida_dirent_node_t * node)
{
    int32_t len;

    if (*root == NULL)
    {
        *root = node;

        return 0;
    }

    for (len = 0; len < (*root)->length; len++)
    {
        if ((*root)->key[len] != node->key[len])
        {
            return ida_dirent_split(local, root, node, len);
        }
    }

    if (node->length > len)
    {
        node->key += len;
        node->length -= len;

        node->parent = *root;

        return ida_dirent_insert(local, &(*root)->childs[*node->key], node);
    }

    if (unlikely((*root)->entry != NULL))
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Duplicate entry in directory");

        return -1;
    }

    (*root)->entry = node->entry;
    node->entry = NULL;

    ida_dirent_free(node);

    return 0;
}

void ida_dirent_destroy_node(ida_dirent_node_t * node)
{
    int32_t i;

    if (node == NULL)
    {
        return;
    }

    for (i = 0; i < 256; i++)
    {
        ida_dirent_destroy_node(node->childs[i]);
    }

    ida_dirent_free(node);
}

void ida_dirent_destroy(ida_dirent_t * root)
{
    int32_t i;

    for (i = 0; i < 256; i++)
    {
        ida_dirent_destroy_node(root->childs[i]);
        root->childs[i] = NULL;
    }
}

void ida_dirent_unassign(ida_dirent_t * dst)
{
    gf_dirent_t * entry, * tmp;

    ida_dirent_destroy(dst);
    if (dst->count > 0)
    {
        list_for_each_entry_safe(entry, tmp, &dst->entries.list, list)
        {
            list_del_init(&entry->list);
            ida_dirent_wipe(entry);
        }
    }
}

int32_t ida_dirent_copy(ida_local_t * local, ida_dirent_t * dst, gf_dirent_t * entries)
{
    gf_dirent_t * entry, * tmp;
    ida_dirent_node_t * node;

    list_for_each_entry(entry, &entries->list, list)
    {
        node = ida_dirent_new(local, entry, NULL, 0);
        if (unlikely(node == NULL))
        {
            goto failed;
        }
        tmp = node->entry;
        if (unlikely(ida_dirent_insert(local, &dst->childs[*node->key], node) != 0))
        {
            goto failed_node;
        }

        list_add_tail(&tmp->list, &dst->entries.list);

        dst->count++;
    }

    return 0;

failed_node:
    ida_dirent_free(node);
failed:
    ida_dirent_unassign(dst);

    return ENOMEM;
}

ida_dirent_node_t * ida_dirent_next(ida_dirent_t * root, ida_dirent_node_t * node)
{
    int32_t i;

    i = 0;
    do
    {
        for (; i < 256; i++)
        {
            if (node->childs[i] != NULL)
            {
                if (node->childs[i]->entry != NULL)
                {
                    return node->childs[i];
                }
                return ida_dirent_next(root, node->childs[i]);
            }
        }

        i = *node->key + 1;

        node = node->parent;
    } while (node != NULL);

    for (; i < 256; i++)
    {
        if (root->childs[i] != NULL)
        {
            if (root->childs[i]->entry != NULL)
            {
                return root->childs[i];
            }
            return ida_dirent_next(root, root->childs[i]);
        }
    }

    return NULL;
}

ida_dirent_node_t * ida_dirent_first(ida_dirent_t * root)
{
    int32_t i;

    for (i = 0; i < 256; i++)
    {
        if (root->childs[i] != NULL)
        {
            if (root->childs[i]->entry != NULL)
            {
                return root->childs[i];
            }
            return ida_dirent_next(root, root->childs[i]);
        }
    }

    return NULL;
}

int32_t ida_dirent_assign(ida_local_t * local, ida_dirent_t * dst, gf_dirent_t * src)
{
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, src, EINVAL);
    if (dst->count == 0)
    {
        INIT_LIST_HEAD(&dst->entries.list);
    }
    return ida_dirent_copy(local, dst, src);
}

int32_t ida_dirent_combine(ida_local_t * local, ida_dirent_t * dst, ida_dirent_t * src)
{
    ida_dirent_node_t * node1, * node2, * node;
    gf_dirent_t * tmp;
    int32_t cmp;

    node1 = ida_dirent_first(dst);
    node2 = ida_dirent_first(src);

    while ((node1 != NULL) && (node2 != NULL))
    {
        cmp = strcmp(node1->entry->d_name, node2->entry->d_name);
        if (cmp < 0)
        {
            node1 = ida_dirent_next(dst, node1);

            continue;
        }

        node = ida_dirent_new(local, node2->entry, NULL, 0);
        if (unlikely(node == NULL))
        {
            return -ENOMEM;
        }

        if (cmp > 0)
        {
            tmp = node->entry;
            if (unlikely(ida_dirent_insert(local, &dst->childs[*node->key], node) != 0))
            {
                ida_dirent_free(node);

                return -EIO;
            }

            list_add_tail(&tmp->list, &dst->entries.list);

            dst->count++;

            node2 = ida_dirent_next(src, node2);
        }
        else
        {
            ida_dirent_merge(local, node1, node);

            node1 = ida_dirent_next(dst, node1);
            node2 = ida_dirent_next(src, node2);
        }
    }
    while (node2 != NULL)
    {
        node = ida_dirent_new(local, node2->entry, NULL, 0);
        if (unlikely(node == NULL))
        {
            return -ENOMEM;
        }
        tmp = node->entry;
        if (unlikely(ida_dirent_insert(local, &dst->childs[*node->key], node) != 0))
        {
            ida_dirent_free(node);

            return -EIO;
        }

        list_add_tail(&tmp->list, &dst->entries.list);

        dst->count++;

        node2 = ida_dirent_next(src, node2);
    }

    return 0;
}

void ida_dirent_clean(ida_local_t * local, ida_dirent_t * dst, int32_t cut)
{
    ida_dirent_node_t * node, * best, * sibling;
    gf_dirent_t * entry;
    off_t offset;

    node = ida_dirent_first(dst);
    while (node != NULL)
    {
        if (node->entry != NULL)
        {
            sibling = best = node;
            while (sibling->siblings != NULL)
            {
                sibling = sibling->siblings;
                if (best->count < sibling->count)
                {
                    list_del_init(&best->entry->list);
                    best = sibling;
                }
                else
                {
                    list_del_init(&sibling->entry->list);
                }
            }
            if (best->count < cut)
            {
                gf_log(local->xl->name, GF_LOG_WARNING, "Entry '%s' removed from directory listing because it is not present or consistent on sufficient subvolumes", best->entry->d_name);

                list_del_init(&best->entry->list);

                dst->count--;
            }
            else
            {
                ida_iatt_adjust(local, &best->entry->d_stat, best->entry->dict, best->entry->inode);
            }
        }

        node = ida_dirent_next(dst, node);
    }

    ida_dirent_destroy(dst);

    offset = 1;
    list_for_each_entry(entry, &dst->entries.list, list)
    {
        entry->d_off = offset;
        offset++;
    }
}
