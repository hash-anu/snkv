/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright 2025 SNKV Contributors */
/*
** kvstore_enc.c — Crypto primitives for SNKV encryption layer.
**
** Uses Monocypher (CC0) for:
**   - Argon2id  : password → 256-bit key derivation
**   - XChaCha20-Poly1305 : per-value AEAD encryption/decryption
**   - crypto_wipe       : secure memory zeroing
**
** Platform random bytes:
**   Linux/macOS/BSD  : getrandom(2) / /dev/urandom
**   Windows          : BCryptGenRandom
*/

#include "kvstore_enc.h"
#include "monocypher/monocypher.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* -------------------------------------------------------------------------
** Platform RNG
** ------------------------------------------------------------------------- */
#ifdef _WIN32
/* RtlGenRandom (aka SystemFunction036) lives in advapi32.dll which is
** linked by default on all Windows toolchains (MSVC and GCC/MinGW).
** No extra -l flag is required. */
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
BOOLEAN NTAPI SystemFunction036(PVOID RandomBuffer, ULONG RandomBufferLength);
#  pragma comment(lib, "advapi32.lib")
static int platformRandBytes(uint8_t *buf, size_t len){
  return SystemFunction036(buf, (ULONG)len) ? 0 : -1;
}
#elif defined(__linux__)
#  include <sys/random.h>
static int platformRandBytes(uint8_t *buf, size_t len){
  size_t done = 0;
  while( done < len ){
    ssize_t r = getrandom(buf + done, len - done, 0);
    if( r < 0 ) return -1;
    done += (size_t)r;
  }
  return 0;
}
#else
/* macOS / BSD / generic POSIX — /dev/urandom */
#  include <stdio.h>
static int platformRandBytes(uint8_t *buf, size_t len){
  FILE *f = fopen("/dev/urandom", "rb");
  if( !f ) return -1;
  size_t r = fread(buf, 1, len, f);
  fclose(f);
  return (r == len) ? 0 : -1;
}
#endif

/* -------------------------------------------------------------------------
** Public helpers
** ------------------------------------------------------------------------- */

int kvstoreEncRandBytes(uint8_t *buf, size_t len){
  return platformRandBytes(buf, len);
}

void kvstoreEncWipe(void *p, size_t n){
  crypto_wipe(p, n);
}

/* -------------------------------------------------------------------------
** kvstoreEncDeriveKey — Argon2id KDF
** ------------------------------------------------------------------------- */
int kvstoreEncDeriveKey(
  uint8_t        aKey[SNKV_ENC_KEY_LEN],
  const void    *pPassword, int nPassword,
  const uint8_t  aSalt[SNKV_ENC_SALT_LEN],
  uint32_t       mCost,
  uint32_t       tCost
){
  /* Argon2id work area: mCost kibibytes */
  size_t workSize = (size_t)mCost * 1024;
  void  *pWork    = malloc(workSize);
  if( !pWork ) return -1;

  crypto_argon2_config cfg;
  cfg.algorithm  = CRYPTO_ARGON2_ID;
  cfg.nb_blocks  = mCost;
  cfg.nb_passes  = tCost;
  cfg.nb_lanes   = 1;

  crypto_argon2_inputs inp;
  inp.pass      = (const uint8_t *)pPassword;
  inp.pass_size = (uint32_t)nPassword;
  inp.salt      = aSalt;
  inp.salt_size = SNKV_ENC_SALT_LEN;

  crypto_argon2(aKey, SNKV_ENC_KEY_LEN, pWork, cfg, inp, crypto_argon2_no_extras);

  /* Wipe work area before freeing */
  crypto_wipe(pWork, workSize);
  free(pWork);
  return 0;
}

/* -------------------------------------------------------------------------
** kvstoreEncEncrypt — XChaCha20-Poly1305 encrypt
** Output layout: [nonce(24)][ciphertext(nPlain)][mac(16)]
** ------------------------------------------------------------------------- */
int kvstoreEncEncrypt(
  uint8_t       *pOut,
  const uint8_t  aKey[SNKV_ENC_KEY_LEN],
  const uint8_t *pPlain, int nPlain
){
  /* Generate fresh random nonce */
  uint8_t *pNonce = pOut;                          /* first 24 bytes */
  if( kvstoreEncRandBytes(pNonce, SNKV_ENC_NONCE_LEN) != 0 ) return -1;

  uint8_t *pCipher = pOut + SNKV_ENC_NONCE_LEN;   /* next nPlain bytes */
  uint8_t *pMac    = pCipher + nPlain;             /* last 16 bytes */

  crypto_aead_lock(pCipher, pMac, aKey, pNonce,
                   NULL, 0,            /* no additional data */
                   pPlain, (size_t)nPlain);
  return 0;
}

/* -------------------------------------------------------------------------
** kvstoreEncDecrypt — XChaCha20-Poly1305 decrypt
** Input layout: [nonce(24)][ciphertext(N)][mac(16)]  where N = nEnc-40
** ------------------------------------------------------------------------- */
int kvstoreEncDecrypt(
  uint8_t       *pOut,
  const uint8_t  aKey[SNKV_ENC_KEY_LEN],
  const uint8_t *pEnc, int nEnc
){
  if( nEnc < SNKV_ENC_OVERHEAD ) return -1;

  const uint8_t *pNonce  = pEnc;
  const uint8_t *pCipher = pEnc + SNKV_ENC_NONCE_LEN;
  const uint8_t *pMac    = pEnc + (nEnc - SNKV_ENC_MAC_LEN);
  int            nPlain  = nEnc - SNKV_ENC_OVERHEAD;

  int r = crypto_aead_unlock(pOut, pMac, aKey, pNonce,
                              NULL, 0,
                              pCipher, (size_t)nPlain);
  /* crypto_aead_unlock returns 0 on success, -1 on MAC mismatch */
  return r;
}
