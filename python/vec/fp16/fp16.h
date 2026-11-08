/* SPDX-License-Identifier: Apache-2.0 */
/*
** fp16/fp16.h — IEEE 754 half <-> single precision conversion.
**
** usearch converts half floats with the compiler's _Float16 type unless
** USEARCH_USE_FP16LIB is set, in which case it includes <fp16/fp16.h> and
** calls the two functions below.  MSVC has no _Float16, so setup.py sets
** USEARCH_USE_FP16LIB=1 there and this header provides the functions.
** Results match _Float16 conversions: round to nearest, ties to even.
*/
#ifndef SNKV_PY_FP16_H
#define SNKV_PY_FP16_H

#include <stdint.h>
#include <string.h>

/* Half (binary16 bits) -> float.  Exact for every input. */
static inline float fp16_ieee_to_fp32_value(uint16_t h)
{
    uint32_t sign = (uint32_t)(h & 0x8000u) << 16;
    uint32_t exp  = (h >> 10) & 0x1Fu;
    uint32_t mant = h & 0x3FFu;
    uint32_t bits;
    float f;

    if (exp == 0) {
        if (mant == 0) {
            bits = sign;                                  /* +-0 */
        } else {
            /* Subnormal: shift until the leading 1 reaches the implicit bit. */
            exp = 127 - 15 + 1;
            while ((mant & 0x400u) == 0) { mant <<= 1; exp--; }
            mant &= 0x3FFu;
            bits = sign | (exp << 23) | (mant << 13);
        }
    } else if (exp == 0x1F) {
        bits = sign | 0x7F800000u | (mant << 13);         /* inf / NaN */
    } else {
        bits = sign | ((exp + (127 - 15)) << 23) | (mant << 13);
    }
    memcpy(&f, &bits, sizeof f);
    return f;
}

/* Float -> half (binary16 bits), round to nearest, ties to even. */
static inline uint16_t fp16_ieee_from_fp32_value(float f)
{
    uint32_t x, sign, mant, half, rem;
    int32_t e;

    memcpy(&x, &f, sizeof x);
    sign = (x >> 16) & 0x8000u;
    mant = x & 0x7FFFFFu;

    if (((x >> 23) & 0xFFu) == 0xFFu) {                   /* inf / NaN */
        return (uint16_t)(sign | 0x7C00u | (mant ? (0x200u | (mant >> 13)) : 0u));
    }
    e = (int32_t)((x >> 23) & 0xFFu) - 127 + 15;
    if (e >= 0x1F) return (uint16_t)(sign | 0x7C00u);     /* overflow -> inf */

    if (e <= 0) {                                         /* subnormal or 0 */
        uint32_t shift, mid;
        if (e < -10) return (uint16_t)sign;               /* below half of 2^-24 */
        mant |= 0x800000u;                                /* implicit 1 */
        shift = (uint32_t)(14 - e);                       /* 14 .. 24 */
        half = mant >> shift;
        rem  = mant & ((1u << shift) - 1u);
        mid  = 1u << (shift - 1u);
        if (rem > mid || (rem == mid && (half & 1u))) half++;
        return (uint16_t)(sign | half);
    }

    half = sign | ((uint32_t)e << 10) | (mant >> 13);
    rem  = mant & 0x1FFFu;
    /* A carry out of the mantissa moves into the exponent, which is the
    ** correct result (including rounding up to infinity). */
    if (rem > 0x1000u || (rem == 0x1000u && (half & 1u))) half++;
    return (uint16_t)half;
}

#endif /* SNKV_PY_FP16_H */
