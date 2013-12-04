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

#include <ctype.h>
#include <sys/uio.h>

#include "defaults.h"
#include "xlator.h"

#include "ida-memory.h"
#include "ida-common.h"
#include "ida-type-dirent.h"
#include "ida-rabin.h"
#include "ida.h"

#define IDA_MAX_NODES 24
#define IDA_MAX_FRAGMENTS 15

static int ida_parse_size(xlator_t * this, uint32_t * nodes, uint32_t * redundancy)
{
    char * size;
    char ** tokens;
    int res, i, token_count;

    res = -1;

    size = NULL;
    GF_OPTION_INIT("size", size, str, done);
    if (unlikely(size == NULL))
    {
        gf_log(this->name, GF_LOG_ERROR, "Option 'size' not defined.");

        goto done;
    }
    if (unlikely(gf_strsplit(size, ":", &tokens, &token_count) != 0))
    {
        gf_log(this->name, GF_LOG_ERROR, "Unable to parse option 'size'.");

        goto done;
    }
    if (unlikely(token_count != 2))
    {
        gf_log(this->name, GF_LOG_ERROR, "Invalid format of option 'size.");

        goto free_tokens;
    }
    if (unlikely(gf_string2uint32(tokens[0], nodes) != 0))
    {
        gf_log(this->name, GF_LOG_ERROR, "Invalid number of nodes in option 'size'.");

        goto free_tokens;
    }
    if (unlikely(gf_string2uint32(tokens[1], redundancy) != 0) ||
        unlikely(*redundancy >= *nodes))
    {
        gf_log(this->name, GF_LOG_ERROR, "Invalid redundancy value in option 'size'.");

        goto free_tokens;
    }
    if (unlikely(*nodes > IDA_MAX_NODES))
    {
        gf_log(this->name, GF_LOG_ERROR, "Too many nodes specified in option 'size'.");

        goto free_tokens;
    }
    if (unlikely(*nodes - *redundancy > IDA_MAX_FRAGMENTS))
    {
        gf_log(this->name, GF_LOG_ERROR, "Unsupported combination of nodes and redundancy in option 'size'.");

        goto free_tokens;
    }

    res = 0;

free_tokens:
    for (i = 0; i < token_count; i++)
    {
        GF_FREE(tokens[i]);
    }
    GF_FREE(tokens);
done:
    return res;
}

static int ida_parse_options(xlator_t * this)
{
    ida_private_t * priv;

    priv = this->private;

    if (unlikely(ida_parse_size(this, &priv->nodes, &priv->redundancy) != 0))
    {
        return -1;
    }
    priv->fragments = priv->nodes - priv->redundancy;
    if (unlikely(priv->redundancy >= priv->fragments))
    {
        gf_log(this->name, GF_LOG_ERROR, "Cannot use a redundancy greater or equeal than the 100%% of the effective space. Use replicate translator instead.");

        return -1;
    }
    priv->node_mask = (1ULL << priv->nodes) - 1ULL;
    priv->block_size = priv->fragments * IDA_GF_BITS * 16;

    return 0;
}

static int ida_prepare_childs(xlator_t * this)
{
    ida_private_t * priv;
    xlator_list_t * child;
    int count;

    priv = this->private;

    count = 0;
    for (child = this->children; child != NULL; child = child->next)
    {
        count++;
    }
    if (unlikely(count == 0))
    {
        gf_log(this->name, GF_LOG_ERROR, "Disperse must have at least one subvolume. Check the volume file.");

        return -1;
    }

    if (unlikely(count != priv->nodes))
    {
        gf_log(this->name, GF_LOG_ERROR, "The number of subvolumes does not match the number of nodes.");

        return -1;
    }

    priv->xl_list = GF_CALLOC(count, sizeof(xlator_t *), gf_ida_mt_xlator_t);
    if (unlikely(priv->xl_list == NULL))
    {
        gf_log(this->name, GF_LOG_ERROR, "Unable to allocate memory for the submodules.");

        return -1;
    }
    priv->xl_up = 0;

    count = 0;
    for (child = this->children; child != NULL; child = child->next)
    {
        priv->xl_list[count] = child->xlator;

        count++;
    }

    return 0;
}

static void __ida_destroy_private(xlator_t * this)
{
    ida_private_t * priv;

    GF_VALIDATE_OR_GOTO("disperse", this, done);

    priv = this->private;
    if (priv != NULL)
    {
        if (priv->xl_list != NULL)
        {
            GF_FREE(priv->xl_list);
            priv->xl_list = NULL;
        }

        GF_FREE(priv);
        this->private = NULL;
    }

done:
    return;
}

int32_t mem_acct_init(xlator_t * this)
{
    int res;

    IDA_VALIDATE_OR_RETURN_ERROR("ida", this, -1);

    res = xlator_mem_acct_init(this, gf_ida_mt_end + 1);
    if (unlikely(res != 0))
    {
        gf_log(this->name, GF_LOG_ERROR, "Memory accounting initialization failed.");
    }

    return res;
}

int32_t reconfigure(xlator_t * this, dict_t * options)
{
    IDA_VALIDATE_OR_RETURN_ERROR("ida", this, -1);

    gf_log(this->name, GF_LOG_WARNING, "Volume online reconfiguration is not supported.");

    return -1;
}

int32_t notify(xlator_t * this, int32_t event, void * data, ...)
{
    ida_private_t * priv;
    int i;

    IDA_VALIDATE_OR_RETURN_ERROR("ida", this, -1);
    IDA_VALIDATE_OR_RETURN_ERROR(this->name, this->private, -1);

    priv = this->private;

    switch (event)
    {
        case GF_EVENT_CHILD_UP:
        case GF_EVENT_CHILD_CONNECTING:
            gf_log(this->name, GF_LOG_DEBUG, "Event CHILD_UP: %s", ((xlator_t *)data)->name);

            LOCK(&priv->lock);

            for (i = 0; i < priv->nodes; i++)
            {
                if (data == priv->xl_list[i])
                {
                    if (unlikely(__sync_fetch_and_or(&priv->xl_up, 1ULL << i) & (1ULL << i)))
                    {
                        gf_log(this->name, GF_LOG_WARNING, "Received CHILD_UP event on a translator that is not down");
                    }
                }
            }

            if (ida_bit_count(priv->xl_up) >= priv->fragments)
            {
                default_notify(this, event, data);
            }

            UNLOCK(&priv->lock);

            break;

        case GF_EVENT_CHILD_DOWN:
            gf_log(this->name, GF_LOG_DEBUG, "Event CHILD_DOWN: %s", ((xlator_t *)data)->name);

            LOCK(&priv->lock);

            for (i = 0; i < priv->nodes; i++)
            {
                if (data == priv->xl_list[i])
                {
                    if (unlikely((__sync_fetch_and_and(&priv->xl_up, ~(1ULL << i)) & (1ULL << i)) == 0))
                    {
                        gf_log(this->name, GF_LOG_WARNING, "Received CHILD_DOWN event on a translator that is not up");
                    }
                }
            }

            if (ida_bit_count(priv->xl_up) < priv->fragments)
            {
                default_notify(this, event, data);
            }

            UNLOCK(&priv->lock);

            break;

        default:
            return default_notify(this, event, data);
    }

    return 0;
}

int32_t init(xlator_t * this)
{
    ida_private_t * priv;

    IDA_VALIDATE_OR_RETURN_ERROR("ida", this, -1);

    if (this->parents == NULL)
    {
        gf_log(this->name, GF_LOG_WARNING, "Volume is dangling. Check the volume file.");
    }

    priv = IDA_MALLOC_OR_RETURN_ERROR(this->name, ida_private_t, -1);
    memset(priv, 0, sizeof(ida_private_t));

    LOCK_INIT(&priv->lock);

    this->private = priv;

    if (unlikely(ida_parse_options(this) != 0))
    {
        gf_log(this->name, GF_LOG_ERROR, "Unable to validate the options of the translator.");

        goto failed;
    }

    if (unlikely(ida_prepare_childs(this) != 0))
    {
        gf_log(this->name, GF_LOG_ERROR, "Unable to configure the submodules of the translator.");

        goto failed;
    }

    ida_rabin_initialize();

    gf_log (this->name, GF_LOG_DEBUG, "disperse xlator loaded");

    return 0;

failed:
    __ida_destroy_private(this);

    return -1;
}

void fini(xlator_t * this)
{
    __ida_destroy_private(this);
}

struct xlator_fops fops =
{
    .lookup      = ida_lookup,
    .stat        = ida_stat,
    .fstat       = ida_fstat,
    .truncate    = ida_truncate,
    .ftruncate   = ida_ftruncate,
    .access      = ida_access,
    .readlink    = ida_readlink,
    .mknod       = ida_mknod,
    .mkdir       = ida_mkdir,
    .unlink      = ida_unlink,
    .rmdir       = ida_rmdir,
    .symlink     = ida_symlink,
    .rename      = ida_rename,
    .link        = ida_link,
    .create      = ida_create,
    .open        = ida_open,
    .readv       = ida_readv,
    .writev      = ida_writev,
    .flush       = ida_flush,
    .fsync       = ida_fsync,
    .opendir     = ida_opendir,
    .readdir     = ida_readdir,
    .readdirp    = ida_readdirp,
    .fsyncdir    = ida_fsyncdir,
    .statfs      = ida_statfs,
    .setxattr    = ida_setxattr,
    .getxattr    = ida_getxattr,
    .fsetxattr   = ida_fsetxattr,
    .fgetxattr   = ida_fgetxattr,
    .removexattr = ida_removexattr,
    .lk          = ida_lk,
    .inodelk     = ida_inodelk,
    .finodelk    = ida_finodelk,
    .entrylk     = ida_entrylk,
    .fentrylk    = ida_fentrylk,
    .rchecksum   = ida_rchecksum,
    .xattrop     = ida_xattrop,
    .fxattrop    = ida_fxattrop,
    .setattr     = ida_setattr,
    .fsetattr    = ida_fsetattr,
};

int32_t ida_forget(xlator_t * this, inode_t * inode)
{
    uint64_t ctx_value;
    ida_inode_ctx_t * ctx;

    if ((inode_ctx_del(inode, this, &ctx_value) == 0) && (ctx_value != 0))
    {
        ctx = (ida_inode_ctx_t *)ctx_value;
        GF_FREE(ctx);
    }

    return 0;
}

int32_t ida_release(xlator_t * this, fd_t * fd)
{
    return 0;
}

int32_t ida_releasedir(xlator_t * this, fd_t * fd)
{
    ida_dir_ctx_t * ctx;
    uint64_t value;

    if (unlikely(fd_ctx_del(fd, this, &value) != 0) || (value == 0))
    {
        gf_log(this->name, GF_LOG_WARNING, "Releasing a directory file descriptor without context");
    }
    else
    {
        ctx = (ida_dir_ctx_t *)(uintptr_t)value;

        ida_dirent_unassign(&ctx->entries);

        GF_FREE(ctx);
    }

    return 0;
}

struct xlator_cbks cbks =
{
    .forget     = ida_forget,
    .release    = ida_release,
    .releasedir = ida_releasedir,
};

struct volume_options options[] =
{
    {
        .key = { "size" },
        .type = GF_OPTION_TYPE_STR,
        .description = "Size of the dispersion represented as X:Y. "
                       "Where X is the number of nodes and Y is the redundancy."
    },
    {
        .key = { "write-extra" },
        .type = GF_OPTION_TYPE_INT,
        .description = "Number of extra answers required to consider an "
                       "operation as valid before propagating the result to the "
                       "upper translators. 0 means that is only required the "
                       "minimum. A negative number means that the answer will "
                       "be propagated before the minimum number of required "
                       "childs have completed. This could improve performance "
                       "but it can be very dangerous and lead to data corruption."
    },
    {
        .key = { "block-size" },
        .type = GF_OPTION_TYPE_INT,
        .description = "File system block size"
    },
    { }
};
