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

#include <string.h>
#include <inttypes.h>

#include "ida-rabin.h"

static uint32_t GfPow[IDA_RABIN_SIZE << 1];
static uint32_t GfLog[IDA_RABIN_SIZE << 1];

void ida_rabin_initialize(void)
{
    uint32_t i;

    GfPow[0] = 1;
    GfLog[0] = IDA_RABIN_SIZE;
    for (i = 1; i < IDA_RABIN_SIZE; i++)
    {
        GfPow[i] = GfPow[i - 1] << 1;
        if (GfPow[i] >= IDA_RABIN_SIZE)
        {
            GfPow[i] ^= IDA_GF_MOD;
        }
        GfPow[i + IDA_RABIN_SIZE - 1] = GfPow[i];
        GfLog[GfPow[i] + IDA_RABIN_SIZE - 1] = GfLog[GfPow[i]] = i;
    }
}

static uint32_t ida_rabin_mul(uint32_t a, uint32_t b)
{
    if (a && b)
    {
        return GfPow[GfLog[a] + GfLog[b]];
    }
    return 0;
}

static uint32_t ida_rabin_div(uint32_t a, uint32_t b)
{
    if (b)
    {
        if (a)
        {
            return GfPow[IDA_RABIN_SIZE - 1 + GfLog[a] - GfLog[b]];
        }
        return 0;
    }
    return IDA_RABIN_SIZE;
}

uint32_t ida_rabin_split(uint32_t size, uint32_t columns, uint32_t row, uint8_t * in, uint8_t * out)
{
    uint32_t i, j;

    size /= 16 * IDA_RABIN_BITS * columns;
    row++;
    for (j = 0; j < size; j++)
    {
        ida_gf_load(in);
        in += 16 * IDA_RABIN_BITS;
        for (i = 1; i < columns; i++)
        {
            ida_gf_mul_table[row]();
            ida_gf_xor(in);
            in += 16 * IDA_RABIN_BITS;
        }
        ida_gf_store(out);
        out += 16 * IDA_RABIN_BITS;
    }

    return size * 16 * IDA_RABIN_BITS;
}

uint32_t ida_rabin_merge(uint32_t size, uint32_t columns, uint32_t * rows, uint8_t ** in, uint8_t * out)
{
    uint32_t i, j, k;
    uint32_t f, off, mask;
    uint8_t inv[16][17];
    uint8_t mtx[16][16];
    uint8_t * p[16];

    size /= 16 * IDA_RABIN_BITS;

    memset(inv, 0, sizeof(inv));
    memset(mtx, 0, sizeof(mtx));
    mask = 0;
    for (i = 0; i < columns; i++)
    {
        inv[i][i] = 1;
        inv[i][columns] = 1;
    }
    k = 0;
    for (i = 0; i < columns; i++)
    {
        while ((mask & 1) != 0)
        {
            k++;
            mask >>= 1;
        }
        mtx[k][columns - 1] = 1;
        for (j = columns - 1; j > 0; j--)
        {
            mtx[k][j - 1] = ida_rabin_mul(mtx[k][j], rows[i] + 1);
        }
        p[k] = in[i];
        k++;
        mask >>= 1;
    }

    for (i = 0; i < columns; i++)
    {
        f = mtx[i][i];
        for (j = 0; j < columns; j++)
        {
            mtx[i][j] = ida_rabin_div(mtx[i][j], f);
            inv[i][j] = ida_rabin_div(inv[i][j], f);
        }
        for (j = 0; j < columns; j++)
        {
            if (i != j)
            {
                f = mtx[j][i];
                for (k = 0; k < columns; k++)
                {
                    mtx[j][k] ^= ida_rabin_mul(mtx[i][k], f);
                    inv[j][k] ^= ida_rabin_mul(inv[i][k], f);
                }
            }
        }
    }
    off = 0;
    for (f = 0; f < size; f++)
    {
        for (i = 0; i < columns; i++)
        {
            ida_gf_load(p[0] + off);
            j = 0;
            while (j < columns)
            {
                k = j + 1;
                while (inv[i][k] == 0)
                {
                    k++;
                }
                ida_gf_mul_table[ida_rabin_div(inv[i][j], inv[i][k])]();
                if (k < columns)
                {
                    ida_gf_xor(p[k] + off);
                }
                j = k;
            }
            ida_gf_store(out);
            out += 16 * IDA_RABIN_BITS;
        }
        off += 16 * IDA_RABIN_BITS;
    }

    return size * 16 * IDA_RABIN_BITS * columns;
}
