/*
 * Author: Benjamin A Schulz
 * License: BSD3
 */
#ifndef QUICKTASK_ALIGNED_MALLOC_H
#define QUICKTASK_ALIGNED_MALLOC_H

#if defined(__GNUC__)
# include <mm_malloc.h>
/* allocate S bytes aligned to A, which must be a power of two */
# define aligned_malloc(S, A) _mm_malloc((S), (A))
# define aligned_free(P) _mm_free((P))
#elif defined(_MSC_VER)
# include <malloc.h>
# define aligned_malloc(S, A) _aligned_malloc((S), (A))
# define aligned_free(P) _aligned_free((P))
#endif
// TODO Use C11 stdalign.h if available
#endif  // QUICKTASK_ALIGNED_MALLOC_H
