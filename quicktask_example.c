/*
 * Author: Benjamin A Schulz
 * License: BSD3
 */

// Compile with
//   gcc quicktask.c quicktask_example.c -O2 -lpthread -lm -o example-bin
// (without pthread on MSYS)

#include "quicktask.h"
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#define Sleep(ms) usleep((ms))
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifndef max
#define max(X, Y) ((X)>=(Y) ? (X) : (Y))
#endif

#define SIZE 400000000
//#define SIZE 1000
volatile float result [SIZE];

static float fun(QuickTask_Index);
static int worker(QuickTask_Range);
static void splitter(QuickTask_Range const*, size_t*, QuickTask_Range*);

float fun(QuickTask_Index xi) {
  float x = (float) (xi + 1);
  return sqrt(x) * sqrt(x*x*x) / sqrt(x*x);  // == abs x when x!=0
}

int worker(QuickTask_Range r) {
  for (QuickTask_Index i = r.first;
       i!=r.end; ++i) {
    result[i] = fun(i);
  }
  return QUICKTASK_SUCCESS;
}

void splitter(QuickTask_Range const *r0,
              size_t *out_n,
              QuickTask_Range *out_range) {
  QuickTask_Index const kMin = max(SIZE / 16, 1);
  QuickTask_Index const f0 = r0->first;
  QuickTask_Index const l = r0->end - f0;
  if (l>kMin) {
    QuickTask_Index const f1 = f0 + l / 2;
    out_range[0].first = f0;
    out_range[0].end = f1;
    out_range[1].first = f1;
    out_range[1].end = r0->end;
    *out_n = 2;
  } else {
    *out_n = 0;
  }
}   

int main() {
  printf("Initializing...\n");
  if (quicktask_init()) {
    printf("init failed\n");
    return 1;
  }
  
  QuickTask_Node *node = 0;
  QuickTask_Range r = { 0, SIZE };
  if (quicktask_insert_task(worker, splitter, r, &node)) {
    printf("insert task failed\n");
    return 1;
  }
  Sleep(250);
  QuickTask_Node *n2 = 0;
  quicktask_insert_task(worker, splitter, r, &n2);
  for (int i = 0; i!=3; ++i) {
    quicktask_print_debug();
    Sleep(1000);
  }
  quicktask_wait_noncoop(node);
  quicktask_wait_noncoop(n2);
  Sleep(10000);
  quicktask_print_debug();
  quicktask_deinit();
  printf("%f\n", result[SIZE-1]);
  return 0;
}
#ifdef __cplusplus
}
#endif
