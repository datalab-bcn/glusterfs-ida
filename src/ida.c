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
#include "gfdfc.h"

#include <ctype.h>
#include <sys/uio.h>

#include "ida-common.h"
#include "ida-mem-types.h"
#include "ida-type-dirent.h"
#include "ida-rabin.h"
#include "ida-manager.h"
#include "ida-combine.h"
#include "ida.h"

#define IDA_MAX_NODES 24
#define IDA_MAX_FRAGMENTS 15

err_t ida_parse_size(xlator_t * this, uint32_t * nodes, uint32_t * redundancy)
{
    char * size;
    char ** tokens;
    int32_t i, token_count;
    err_t error;

    error = 0;

    size = NULL;
    GF_OPTION_INIT("size", size, str, done);
    SYS_TEST(
        size != NULL,
        ENOENT,
        E(),
        LOG(E(), "Option 'size' not defined."),
        GOTO(done, &error)
    );
    SYS_CODE(
        gf_strsplit, (size, ":", &tokens, &token_count),
        EINVAL,
        E(),
        LOG(E(), "Unable to parse option 'size'."),
        GOTO(done, &error)
    );
    SYS_TEST(
        token_count == 2,
        EINVAL,
        E(),
        LOG(E(), "Invalid format of option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_CODE(
        gf_string2uint32, (tokens[0], nodes),
        EINVAL,
        E(),
        LOG(E(), "The number of nodes is invalid in option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_CODE(
        gf_string2uint32, (tokens[1], redundancy),
        EINVAL,
        E(),
        LOG(E(), "The level of redundancy is invalid in option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_TEST(
        *redundancy < *nodes,
        EINVAL,
        E(),
        LOG(E(), "Invalid redundancy value in option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_TEST(
        *nodes <= IDA_MAX_NODES,
        EINVAL,
        E(),
        LOG(E(), "Too many nodes specified in option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_TEST(
        *nodes - *redundancy <= IDA_MAX_FRAGMENTS,
        EINVAL,
        E(),
        LOG(E(), "Unsupported combination of nodes and redundancy in option "
                 "'size'."),
        GOTO(free_tokens, &error)
    );

free_tokens:
    for (i = 0; i < token_count; i++)
    {
        SYS_FREE(tokens[i]);
    }
    SYS_FREE(tokens);
done:
    return error;
}

err_t ida_parse_options(xlator_t * this)
{
    ida_private_t * priv;

    priv = this->private;

    SYS_CALL(
        ida_parse_size, (this, &priv->nodes, &priv->redundancy),
        E(),
        RETERR()
    );

    priv->fragments = priv->nodes - priv->redundancy;
    SYS_TEST(
        priv->redundancy < priv->fragments,
        EINVAL,
        E(),
        LOG(E(), "Cannot use a redundancy greater or equal than 100%% of the "
                 "effective space. Use replicated volume instead."),
        RETERR()
    );

    priv->node_mask = (1ULL << priv->nodes) - 1ULL;
    priv->block_size = priv->fragments * IDA_GF_BITS * 16;

    return 0;
}

err_t ida_prepare_childs(xlator_t * this)
{
    ida_private_t * priv;
    xlator_list_t * child;
    int32_t count;

    priv = this->private;

    count = 0;
    for (child = this->children; child != NULL; child = child->next)
    {
        count++;
    }
    SYS_TEST(
        count >= 3,
        EINVAL,
        E(),
        LOG(E(), "A dispersed volume must have three subvolumes at least."),
        RETERR()
    );
    SYS_TEST(
        count == priv->nodes,
        EINVAL,
        E(),
        LOG(E(), "The number of subvolumes does not match the number of "
                 "configured nodes."),
        RETERR()
    );

    SYS_CALLOC0(
        &priv->xl_list, count, gf_ida_mt_xlator_t,
        E(),
        RETERR()
    );
    priv->xl_up = 0;

    count = 0;
    for (child = this->children; child != NULL; child = child->next)
    {
        priv->xl_list[count++] = child->xlator;
    }

    return 0;
}

void __ida_destroy_private(xlator_t * this)
{
    ida_private_t * priv;

    priv = this->private;
    if (priv != NULL)
    {
        if (priv->xl_list != NULL)
        {
            SYS_FREE(priv->xl_list);
            priv->xl_list = NULL;
        }

        SYS_FREE(priv);
        this->private = NULL;
    }
}

int32_t mem_acct_init(xlator_t * this)
{
    SYS_CODE(
        xlator_mem_acct_init, (this, gf_ida_mt_end + 1),
        ENOMEM,
        E(),
        LOG(E(), "Memory accounting initialization failed."),
        RETVAL(-1)
    );

    return 0;
}

int32_t reconfigure(xlator_t * this, dict_t * options)
{
    logE("Online volume reconfiguration is not supported.");

    return -1;
}

int32_t notify(xlator_t * this, int32_t event, void * data, ...)
{
    ida_private_t * priv;

    priv = this->private;

    return dfc_default_notify(priv->dfc, this, event, data);
}

void dfc_notify(dfc_t * dfc, xlator_t * xl, int32_t event)
{
    ida_private_t * priv;
    int32_t i;

    priv = dfc->xl->private;

    switch (event)
    {
        case DFC_CHILD_UP:
            logI("Event CHILD_UP: %s", xl->name);

            for (i = 0; i < priv->nodes; i++)
            {
                if (xl == priv->xl_list[i])
                {
                    priv->xl_up |= 1ULL << i;
                }
            }

            // TODO: should be priv->fragments
            if (dfc->active == priv->nodes)
            {
                logI("Going UP");
                default_notify(dfc->xl, GF_EVENT_CHILD_UP, NULL);
            }

            break;

        case DFC_CHILD_DOWN:
            logI("Event CHILD_DOWN: %s", xl->name);

            for (i = 0; i < priv->nodes; i++)
            {
                if (xl == priv->xl_list[i])
                {
                    priv->xl_up &= ~(1ULL << i);
                }
            }

            // TODO: should be priv->fragments
            if (dfc->active == priv->nodes - 1)
            {
                logI("Going DOWN");
                default_notify(dfc->xl, GF_EVENT_CHILD_DOWN, NULL);
            }

            break;
    }
}

int32_t init(xlator_t * this)
{
    ida_private_t * priv;

    if (this->parents == NULL)
    {
        logW("Volume does not have parents.");
    }

    SYS_MALLOC0(
        &priv, gf_ida_mt_ida_private_t,
        E(),
        RETVAL(-1)
    );

    LOCK_INIT(&priv->lock);

    priv->xl = this;

    this->private = priv;

    SYS_CALL(
        ida_parse_options, (this),
        E(),
        GOTO(failed)
    );
    SYS_CALL(
        ida_prepare_childs, (this),
        E(),
        GOTO(failed)
    );

    ida_rabin_initialize();

    SYS_CALL(
        gfsys_initialize, (NULL, false),
        E(),
        GOTO(failed)
    );

    SYS_CALL(
        dfc_initialize, (this, 64, 16, dfc_notify, &priv->dfc),
        E(),
        GOTO(failed)
    );

    logD("Disperse translator loaded.");

    return 0;

failed:
    __ida_destroy_private(this);

    return -1;
}

void fini(xlator_t * this)
{
    __ida_destroy_private(this);
}

#define IDA_FOP_MODE_INC_DIO 0
#define IDA_FOP_MODE_INC_DFC 0
#define IDA_FOP_MODE_ALL_DIO 0
#define IDA_FOP_MODE_ALL_DFC 1
#define IDA_FOP_MODE_MIN_DIO 0
#define IDA_FOP_MODE_MIN_DFC 1
#define IDA_FOP_MODE_MOD_DIO 0
#define IDA_FOP_MODE_MOD_DFC 0

#define IDA_FOP_DISPATCH_INC incremental
#define IDA_FOP_DISPATCH_ALL all
#define IDA_FOP_DISPATCH_MIN minimum
#define IDA_FOP_DISPATCH_MOD write

#define IDA_FOP_COUNT_INC 0
#define IDA_FOP_COUNT_MIN 1
#define IDA_FOP_COUNT_ALL 2
#define IDA_FOP_COUNT_MOD 2

#define IDA_FOP(_fop, _num, _dfc) \
    static ida_answer_t * __ida_copy_##_fop( \
                  SYS_ARGS_DECL((SYS_GF_ARGS_CBK, SYS_GF_ARGS_##_fop##_cbk))) \
    { \
        uintptr_t * ans; \
        ans = SYS_GF_CBK_CALL(_fop, IDA_ANS_SIZE); \
        return (ida_answer_t *)ans; \
    } \
    static ida_answer_t * ida_copy_##_fop(uintptr_t * io) \
    { \
        SYS_GF_WIND_CBK_TYPE(_fop) * args; \
        args = (SYS_GF_WIND_CBK_TYPE(_fop) *)io; \
        return __ida_copy_##_fop( \
                    SYS_ARGS_LOAD( \
                        args, \
                        (SYS_GF_ARGS_CBK, SYS_GF_ARGS_##_fop##_cbk) \
                    ) \
               ); \
    } \
    static ida_handlers_t ida_handlers_##_fop = \
    { \
        .dispatch = SYS_GLUE(ida_dispatch_, IDA_FOP_DISPATCH_##_num), \
        .combine  = SYS_GLUE(ida_combine_, _fop), \
        .rebuild  = SYS_GLUE(ida_rebuild_, _fop), \
        .copy     = SYS_GLUE(ida_copy_, _fop) \
    }; \
    SYS_ASYNC_CREATE(__ida_##_fop, ((dfc_transaction_t *, txn), \
                                    (call_frame_t *, frame), \
                                    (xlator_t *, this), \
                                    SYS_GF_ARGS_##_fop)) \
    { \
        ida_private_t * ida; \
        ida_request_t * req; \
        ida = this->private; \
        req = (ida_request_t *)SYS_GF_FOP_CALL(_fop, IDA_REQ_SIZE); \
        req->frame = frame; \
        req->xl = this; \
        req->handlers = &ida_handlers_##_fop; \
        req->txn = txn; \
        req->sent = 0; \
        req->completed = 0; \
        sys_lock_initialize(&req->lock); \
        INIT_LIST_HEAD(&req->answers); \
        switch (IDA_FOP_COUNT_##_num) \
        { \
            case IDA_FOP_COUNT_INC: req->required = 1; break; \
            case IDA_FOP_COUNT_MIN: req->required = ida->fragments; break; \
            case IDA_FOP_COUNT_ALL: req->required = ida->nodes; break; \
        } \
        req->pending = 0; \
        ida_prepare_##_fop(ida, req); \
        ida_handlers_##_fop.dispatch(ida, req); \
    } \
    int32_t ida_##_fop(call_frame_t * frame, xlator_t * xl, \
                       SYS_ARGS_DECL((SYS_GF_ARGS_##_fop))) \
    { \
        dfc_transaction_t * txn = NULL; \
        ida_private_t * ida = xl->private; \
        sys_dict_acquire(&xdata, xdata); \
        if (IDA_FOP_MODE_##_num##_##_dfc != 0) \
        { \
            SYS_CALL( \
                dfc_begin, (ida->dfc, &txn), \
                E(), \
                GOTO(failed) \
            ); \
            SYS_CALL( \
                dfc_attach, (txn, &xdata), \
                E(), \
                GOTO(failed_txn) \
            ); \
        } \
        SYS_ASYNC(__ida_##_fop, (txn, frame, xl, \
                                 SYS_ARGS_NAMES((SYS_GF_ARGS_##_fop)))); \
        sys_dict_release(xdata); \
        return 0; \
    failed_txn: \
        dfc_end(txn, 0); \
    failed: \
        logE("IDA: init failed"); \
        sys_gf_##_fop##_unwind_error(frame, EIO, NULL); \
        sys_dict_release(xdata); \
        return 0; \
    }

IDA_FOP(access,       INC, DIO)
IDA_FOP(create,       ALL, DFC)
IDA_FOP(entrylk,      ALL, DFC)
IDA_FOP(fentrylk,     ALL, DFC)
IDA_FOP(flush,        ALL, DIO)
IDA_FOP(fsync,        ALL, DFC)
IDA_FOP(fsyncdir,     ALL, DIO)
IDA_FOP(getxattr,     INC, DIO)
IDA_FOP(fgetxattr,    INC, DIO)
IDA_FOP(inodelk,      ALL, DFC)
IDA_FOP(finodelk,     ALL, DFC)
IDA_FOP(link,         ALL, DFC)
IDA_FOP(lk,           ALL, DFC)
IDA_FOP(lookup,       ALL, DFC)
IDA_FOP(mkdir,        ALL, DFC)
IDA_FOP(mknod,        ALL, DFC)
IDA_FOP(open,         ALL, DFC)
IDA_FOP(opendir,      ALL, DFC)
IDA_FOP(rchecksum,    MIN, DFC)
IDA_FOP(readdir,      INC, DIO)
IDA_FOP(readdirp,     INC, DIO)
IDA_FOP(readlink,     INC, DIO)
IDA_FOP(readv,        MIN, DFC)
IDA_FOP(removexattr,  ALL, DFC)
IDA_FOP(fremovexattr, ALL, DFC)
IDA_FOP(rename,       ALL, DFC)
IDA_FOP(rmdir,        ALL, DFC)
IDA_FOP(setattr,      ALL, DFC)
IDA_FOP(fsetattr,     ALL, DFC)
IDA_FOP(setxattr,     ALL, DFC)
IDA_FOP(fsetxattr,    ALL, DFC)
IDA_FOP(stat,         INC, DIO)
IDA_FOP(fstat,        INC, DIO)
IDA_FOP(statfs,       ALL, DIO)
IDA_FOP(symlink,      ALL, DFC)
IDA_FOP(truncate,     ALL, DFC)
IDA_FOP(ftruncate,    ALL, DFC)
IDA_FOP(unlink,       ALL, DFC)
IDA_FOP(writev,       MOD, DFC)
IDA_FOP(xattrop,      ALL, DFC)
IDA_FOP(fxattrop,     ALL, DFC)

int32_t ida_forget(xlator_t * this, inode_t * inode)
{
    uint64_t ctx_value;
    ida_inode_ctx_t * ctx;

    if ((inode_ctx_del(inode, this, &ctx_value) == 0) && (ctx_value != 0))
    {
        ctx = (ida_inode_ctx_t *)ctx_value;
        SYS_FREE(ctx);
    }

    return 0;
}

int32_t ida_invalidate(xlator_t * this, inode_t * inode)
{
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
        logW("Releasing a directory file descriptor without context");
    }
    else
    {
        ctx = (ida_dir_ctx_t *)(uintptr_t)value;

//        ida_dirent_unassign(&ctx->entries);

        SYS_FREE(ctx);
    }

    return 0;
}

SYS_GF_FOP_TABLE(ida);
SYS_GF_CBK_TABLE(ida);

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
