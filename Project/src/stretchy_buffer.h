#ifndef STRECHY_BUFFER_H
#define STRECHY_BUFFER_H

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <assert.h>

typedef struct BufHdr {
    size_t len;
    size_t cap;
    char buf[0];
} BufHdr;

#define MAX(x, y) (((x) > (y)) ? (x) : (y))

#define buf__hdr(b) ((BufHdr *)((char *) b - offsetof(BufHdr,buf)))
#define buf__fits(b, n) (buf_len(b) + (n) <= buf_cap(b))
#define buf__fit(b, n) (buf__fits(b, n) ? 0 : ((b) = buf__grow((b), buf_len(b) + (n), sizeof(*(b)))))

#define buf_len(b) ((b) ? buf__hdr(b)->len : 0)
#define buf_cap(b) ((b) ? buf__hdr(b)->cap : 0)
#define buf_push(b, x) (buf__fit(b, 1), b[buf_len(b)] = (x), buf__hdr(b)->len++)
#define buf_free(b) ((b) ? free(buf__hdr(b)) : 0)

#define buf_remove(b, index) do {\
                    for (size_t i = index ; i < buf_len(b) - 1 ; i++) {\
                        b[i] = b[i + 1];\
                    }\
                    buf__hdr(b)->len--;\
                } while(0)

void *buf__grow(const void *buf, size_t new_len, size_t elem_size);

#endif