/* 
 * Quicktask
 *
 * Author: Benjamin A Schulz
 * License: BSD3
 *
 */
#ifndef QUICKTASK_QUICKTASK_H
#define QUICKTASK_QUICKTASK_H
#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdlib.h>

/* Macros */
#define QUICKTASK_TYPEDEF_STRUCT(T) typedef struct T T

/*
 * Error codes
 */
#define QUICKTASK_SUCCESS 0
#define QUICKTASK_INVALID_ARG                   0x60000000
#define QUICKTASK_MEMORY_ALLOCATION_FAILED      0x60100000
#define QUICKTASK_ALREADY_INITIALIZED           0x60002000
#define QUICKTASK_WORKER_THREAD_CREATION_FAILED 0x60102010

/*
 * Types
 */
struct QuickTask_Node;  /* Oblique node type */
QUICKTASK_TYPEDEF_STRUCT(QuickTask_Node);

typedef uint32_t QuickTask_Index;

struct QuickTask_Range {
  QuickTask_Index first;
  QuickTask_Index end;
};
QUICKTASK_TYPEDEF_STRUCT(QuickTask_Range);

typedef int (*QuickTask_TaskFun)(QuickTask_Range);
typedef void (*QuickTask_SplitFun)(QuickTask_Range const * /*in*/,
                                   size_t* /*out_n*/,
                                   QuickTask_Range* /*out_ranges*/);

/*
 * Functions
 */
int /* error code */ quicktask_init(void);
int /* error code */ quicktask_deinit(void);

int /* error code */
quicktask_insert_task(QuickTask_TaskFun,
                      QuickTask_SplitFun,
                      QuickTask_Range,
                      QuickTask_Node** /*out*/);

int /* boolean */ quicktask_is_done(QuickTask_Node *);

int /* error code */ quicktask_wait_noncoop(QuickTask_Node volatile*);
int /* error code */ quicktask_wait_coop(QuickTask_Node volatile*);

void quicktask_print_debug();

#ifdef __cplusplus
}
#endif
#endif  /* QUICKTASK_QUICKTASK_H */
