/*
 * Quicktask
 *
 * Author: Benjamin A Schulz
 * License: BSD3
 *
 */

// This file is C99 with compiler-specific extensions for x86 synchronization
// and high-resolution sleep. Only GCC is currently supported, with
// MSYS/MingW on Windows or natively on Linux. If strict c99 or c11
// compliance is desired, you can define QUICKTASK_GCC_NO_ASM for a
// theoretical increase in spinlock contention.
//
// * Quicktask
// ** #includes
#include "quicktask.h"
#include "aligned_malloc.h"
#include <stdlib.h>
#include <stdio.h>

#ifdef _WIN32
#include <process.h>  // _beginthreadex, _endthreadex, uintptr_t
#include <windows.h>  // Sleep, CloseHandle
#else
#include <pthread.h>
#include <unistd.h>  // usleep
#endif

#ifdef __cplusplus
extern "C" {
#endif

// ** #defines
// TODO(bas) Add target-specific clauses for pointer size
//#define QUICKTASK_4_BYTE_PTR 1

#define NUM_THREADS 6
#define CL_SIZE 64

#define cla_malloc(S) aligned_malloc((S), CL_SIZE)
#define cla_free(P) aligned_free((P))

#if defined(__GNUC__)
# define atomic_test_set(P, V) __sync_lock_test_and_set((P), (V))
# define atomic_try_lock(P) !atomic_test_set((P), 1)
# define atomic_unlock(P) __sync_lock_release((P))
# ifndef QUICKTASK_GCC_NO_ASM
#   define cpu_pause() __asm__ volatile("pause\n": : :"memory")
# else
#   define cpu_pause()
# endif  // !QUICKTASK_GCC_NO_ASM
# define full_memory_barrier() __sync_synchronize()
#elif defined(_MSC_VER)
# include <Windows.h>
# define atomic_try_lock(P)  !_InterlockedCompareExchange16((P), 1, 0)
# define atomic_unlock(P) _InterlockedCompareExchange16((P), 0, 1)
# define cpu_pause() __asm pause
# define full_memory_barrier() MemoryBarrier()
#endif

#ifdef _WIN32
// OS-specific
typedef void (*ThreadFun)();
#define THREADCALL __stdcall
#define thread_sleep(MS) Sleep((MS))
typedef uintptr_t ThreadHandle;
int begin_thread(ThreadFun, ThreadHandle*);
int begin_thread(Fun f, ThreadHandle *h) {
  if (*h = _beginthreadex(0, 0, (f), 0, 0, 0)) {
    return QUICKTASK_WORKER_THREAD_CREATION_FAILED;
  }
  return QUICKTASK_SUCCESS;
}
#define WORKER_FUN(f) (f)
void end_thread(void*);
void end_thread(void *rv) {
  _endthreadex((unsigned int) rv);
}
void release_thread(ThreadHandle);
void release_thread(ThreadHandle t) {
  CloseHandle(t);
}

#else  // POSIX presumed
typedef void *(*ThreadFun)(void*);
#define THREADCALL
#define thread_sleep(MS) usleep(1000*(MS))
typedef pthread_t ThreadHandle;
#define LINUX_DUMMY_THREAD_FUN(f) void *f##_LINUX_DUMMY(void*); \
  void *f##_LINUX_DUMMY(void *unused) { \
    (f)(0); \
    return 0; \
  }
#define WORKER_FUN(f) f##_LINUX_DUMMY
int begin_thread(ThreadFun, ThreadHandle*);
int begin_thread(ThreadFun f, ThreadHandle *h) {
  if (pthread_create(h, 0, f, 0)) {
    return QUICKTASK_WORKER_THREAD_CREATION_FAILED;
  }
  return QUICKTASK_SUCCESS;
}
void end_thread(void*);
void end_thread(void *rv) {
  pthread_exit(rv);
}
void release_thread(ThreadHandle);
void release_thread(ThreadHandle t) {
  void *ignored;
  pthread_join(t, &ignored);
}
#endif
// End OS-specific
// ** Types

typedef volatile int8_t Mutex;

enum StateEnum {
  STATE_FREE     = 0x00,
  STATE_DONE     = 0x01,  //  1  17
  STATE_OPEN     = 0x0A,  // 10  26
  STATE_ASSIGNED = 0x0B,  // 11  27
//  STATE_ERROR    = 0x0E,
  STATE_SPLIT    = 0x10
};

struct RootHolder {
  Mutex mx;  // While the root mutex is locked, the pointed-to node will not be recycled.
  int8_t padding_ [7];  // explicit padding
  volatile QuickTask_Node *ptr;
};
QUICKTASK_TYPEDEF_STRUCT(RootHolder);

struct WorkerState {
  volatile int8_t is_interrupted;
  size_t my_index;
};
QUICKTASK_TYPEDEF_STRUCT(WorkerState);

struct ThreadMgrState {
  Mutex mx;
  int16_t num_workers;
  ThreadHandle recycler_handle;
  volatile WorkerState *recycler_state;
  ThreadHandle *thread_handles;
  WorkerState volatile **worker_states;
};
QUICKTASK_TYPEDEF_STRUCT(ThreadMgrState);

union Payload {
  QuickTask_Range volatile range;
  QuickTask_Node volatile *child_node;
};
typedef union Payload Payload;

struct TaskDetail {
  QuickTask_TaskFun fun;                    //     8
#ifdef QUICKTASK_4_BYTE_PTR
  int8_t fun_padding_ [4];
#endif
  QuickTask_SplitFun split;                 //     8
#ifdef QUICKTASK_4_BYTE_PTR
  int8_t split_padding_ [4];
#endif
  Payload q;                                //     8
};
QUICKTASK_TYPEDEF_STRUCT(TaskDetail);

struct QuickTask_Node {         // offset size
  Mutex mx;                         //  0  1
  volatile uint8_t ref_count;       //  1  1
  volatile int8_t parent_ix;        //  2  1
  int8_t unused;                    //  3  1
  volatile uint8_t state [2];       //  4  2*1
  volatile uint8_t user_error [2];  //  6  2*1
  volatile QuickTask_Node *parent;  //  8  8
#ifdef QUICKTASK_4_BYTE_PTR
  int8_t parent_padding_ [4];
#endif
  volatile TaskDetail tasks [2];    // 16 48
};

struct ListNode {
  void *next;
  void *x;
};
QUICKTASK_TYPEDEF_STRUCT(ListNode);

// ** Quicktask globals
static volatile ThreadMgrState *State;
static volatile RootHolder     *RootH;

// ** Local function declarations
static int try_lock(Mutex*);
static void lock(Mutex*);
static void unlock(Mutex*);
static int try_enter_tree(QuickTask_Node**);
static unsigned int THREADCALL worker(void*);
static int check_has_open(QuickTask_Node*, size_t*);
static int check_has_free(QuickTask_Node*, size_t*);
static int check_has_split(QuickTask_Node*, size_t*);
static int check_has_split_open(QuickTask_Node*, size_t*);
static int is_done(uint8_t);
static int is_open(uint8_t);
static int is_split_open(uint8_t);
static int is_split(uint8_t);
static int is_free(uint8_t);
static void is_split_arr(QuickTask_Node*, int[]);
static void set_task(QuickTask_Node*, size_t, QuickTask_TaskFun, QuickTask_SplitFun,
              QuickTask_Range);
static int take_free_node(QuickTask_Node**);
static void set_child(QuickTask_Node*, size_t, QuickTask_Node const*);
static void print_set_child_debug(QuickTask_Node const*, QuickTask_Node const*);
static void init_node(QuickTask_Node*);
static int do_work(QuickTask_Node*, TaskDetail*, size_t);
static void init_worker_state(WorkerState*);
static void deinit_worker_state(WorkerState*);
static void report_done(QuickTask_Node**, size_t);
static void report_error(QuickTask_Node**, size_t);
static int try_join(QuickTask_Node**, TaskDetail*, size_t*, WorkerState*);
static void print_node(QuickTask_Node const*);
static void split_rec(QuickTask_Node**, TaskDetail*, size_t*);
static int try_descent_to_task(QuickTask_Node**, TaskDetail*, size_t*);
static int try_ascent_to_split(QuickTask_Node**);
static void interrupt_workers(void);
static void interruption_point(WorkerState volatile*);
static void release_node(QuickTask_Node*);
static int are_all_done(QuickTask_Node volatile*);
static int list_append(ListNode**, void*);
static int list_try_next(ListNode**);
static void list_free(ListNode*);
static int append_from_tree(QuickTask_Node*, ListNode**);
static int recycle(size_t*);
static int recycle_from(QuickTask_Node*, size_t*);
static void reclaim_leaves(ListNode*, size_t*);
static unsigned int THREADCALL recycle_worker(void*);
static void interrupt_recycler(void);

// ** Functions
// *** The recycler functions
// These functions deal with ushering left-over nodes into the next world:
// The free -list- heap. The recycle* functions cannot be called by multiple
// threads; so, a dedicated thread is used.
//
// We actually collect most nodes directly through the worker threads in
// report_done(). Almost all nodes are collected that way. The recycler takes
// care of the detritus that's left when nodes couldn't be collected
// immediately due to the unwelcome presence of a loitering thread.
//
// TODO Change to always collect via report_done()
// TODO Collect directly in ascent_to_split() as well to get rid of
// the recycler altogether
//
// **** list_append()
// If `*inout_n' is non-null, inserts a new node after the passed-in node.
// If `*inout_n' is 0, populates a new first node.
int list_append(ListNode **inout_n, void *x) {
#ifdef QUICKTASK_DEBUG
  if (!inout_n) {
    printf("inout_n is null\n");
    return QUICKTASK_INVALID_ARG;
  }
#endif
  ListNode *const n = *inout_n;
  ListNode *const m = malloc(sizeof(*m));
  if (!m) {
    return QUICKTASK_MEMORY_ALLOCATION_FAILED;
  }
  m->next = 0;
  m->x = x;
  if (n) {
    n->next = m;
  }
  *inout_n = m;
  return QUICKTASK_SUCCESS;
}
// **** list_try_next()
// Move to the next node in the list if there is one and
// return 1. Otherwise return 0.
int list_try_next(ListNode **inout_n) {
#ifdef QUICKTASK_DEBUG
  if (!inout_n) {
    printf("Trying to advance from nullptr to list node.\n");
    return 0;
  }
  if (!(*inout_n)) {
    printf("ListNode n is null.\n");
    return 0;
  }
#endif
  ListNode *n = *inout_n;
  if (n->next) {
    *inout_n = n->next;
    return 1;
  }
  return 0;
}
// **** list_free()
// Free `n' and all subsequent list nodes.
void list_free(ListNode *n) {
  if (!n) {
    return;
  }
  while (n) {
    ListNode *next = n->next;
    n->next = 0;
    free(n);
    n = next;
  }
}
// **** append_from_tree()
// Appends all children from the QuickTask_Node to the list.
int append_from_tree(QuickTask_Node *tn, ListNode **inout_last) {
  int has_child[2] = { 0, 0 };
  QuickTask_Node *cs[2] = { 0, 0 };
  lock(&tn->mx);
  has_child[0] = is_split(tn->state[0]);
  has_child[1] = is_split(tn->state[1]);
  cs[0] = has_child[0] ? (QuickTask_Node*) tn->tasks[0].q.child_node : 0;
  cs[1] = has_child[1] ? (QuickTask_Node*) tn->tasks[1].q.child_node : 0;
  unlock(&tn->mx);
  int ec = QUICKTASK_SUCCESS;
  if (!has_child[0] && !has_child[1]) {
    //printf("no children\n");
    return ec;
  }
  if (has_child[0]) {
    ec = list_append(inout_last, (void*) cs[0]);
  }
  if (ec) {
    return ec;
  }
  if (has_child[1]) {
    ec = list_append(inout_last, (void*) cs[1]);
  }
  return ec;
}
// **** recycle_from()
int recycle_from(QuickTask_Node *n, size_t *out_n) {
  if (!n) {
    return QUICKTASK_INVALID_ARG;
  }
  *out_n = 0;
  QuickTask_Node *t0 = n;
  // Collect the done leaves from the tree under t0.
  //
  // Initialize two lists. Fill the first list with the children of t0.
  // Then loop to fill the second list with the children of the first
  // list. Check and reclaim done leaves. Rinse and repeat.
  //printf("\n***RECYCLING***\n");
  //printf("  (debug before:)\n");
  //quicktask_print_debug();
  ListNode *l0 = 0;
  int ec = list_append(&l0, (void*) 0);  // First node in first list
  if (ec) {
    return ec;
  }
  ListNode *l0_i = l0;  // Cursor node in first list
  ListNode *l1 = 0;
  ec = list_append(&l1, (void*) 0);
  if (ec) {
    return ec;
  }

  append_from_tree(t0, &l0_i);

  if (l0==l0_i) {
    list_free(l0);
    list_free(l1);
    return QUICKTASK_SUCCESS;
  }
  l0_i = l0->next;  // first non-dummy
  for (;;) {
    //printf(">OUTERLOOP<\n");
    ListNode *l1_i = l1;
    l0_i = l0;
    if (!l0->next) {
      //printf("breaking 1\n");
      break;
    }
    //size_t i = 0;
    while (list_try_next(&l0_i)) {
      //printf(">>INNERLOOP<< (%p)\n", l0_i->x);
      QuickTask_Node *n = (QuickTask_Node*) l0_i->x;
      //printf("n is %p\n", n);
      append_from_tree(n, &l1_i);
      //printf("l1_i: %p\n", l1_i->x);
      //++i;
    }
    //printf("NUM %zu\n", i);
    reclaim_leaves(l0->next, out_n);
    list_free(l0->next);
    l0->next = l1->next;
    l1->next = 0;
    //printf("l0->next is %p\n", l0->next);
    if (l1_i==l1) {
      //printf("breaking 2\n");
      break;
    }
  }
  //printf("freeing\n");
  list_free(l0);
  list_free(l1);
  //quicktask_print_debug();
  //printf("***END RECYCLING***\n\n");
  return QUICKTASK_SUCCESS;
}
// **** recycle()
int recycle(size_t *out_n) {
  lock(&RootH->mx);
  QuickTask_Node *t0 = (QuickTask_Node*) RootH->ptr;
  // Keep a ref in case the tree grows upward, which means
  // t0 could be collected by another thread otherwise.
  if (!try_lock(&t0->mx)) {
    unlock(&RootH->mx);
    return QUICKTASK_SUCCESS;
  }
  ++(t0->ref_count);
  unlock(&t0->mx);
  unlock(&RootH->mx);

  int ret = recycle_from(t0, out_n);

  lock(&t0->mx);
  --(t0->ref_count);
  unlock(&t0->mx);

  return ret;
}
// **** reclaim_leaves()
void reclaim_leaves(ListNode *ln, size_t *out_n) {
  if (!ln) {
    return;
  }
  do {
    QuickTask_Node *n = (QuickTask_Node*) ln->x;
    //printf("r_l(): %p\n", n);
    if (!n) {
      return;
    }
    if ((STATE_DONE==n->state[0] || STATE_FREE==n->state[0]) &&
        (STATE_DONE==n->state[1] || STATE_FREE==n->state[1])) {
      QuickTask_Node *p = (QuickTask_Node*) n->parent;
      if (p) {
        if (!try_lock(&p->mx)) {
          printf("Couldn't lock p\n");
          return;
        }
        if (!try_lock(&n->mx)) {
          unlock(&p->mx);
          printf("Couldn't lock n\n");
          return;
        }
        if ((p->ref_count || n->ref_count) ||
            (p!=n->parent) ||
            !(STATE_DONE==n->state[0] || STATE_FREE==n->state[0]) ||
            !(STATE_DONE==n->state[1] || STATE_FREE==n->state[1])) {
          unlock(&n->mx);
          unlock(&p->mx);
          //printf("Didn't reclaim (inner)\n");
        } else {
          int8_t pix = n->parent_ix;
          p->state[pix] = STATE_DONE;
          p->tasks[pix].q.child_node = 0;
          // No need to unlock n as no one waits on it.
          unlock(&p->mx);
          n->parent = 0;
          ++(*out_n);
          printf("RECLAIMED leaf %p\n", (void*) n);
          release_node(n);
        }
      }
    }
  } while (list_try_next(&ln));
  //printf("Leaving r_l()\n");
}
// **** recycle_worker()
unsigned int THREADCALL recycle_worker(void *dummy) {
  WorkerState state;
  state.is_interrupted = 0;
  lock(&State->mx);
  State->recycler_state = &state;
  unlock(&State->mx);
  int ec = QUICKTASK_SUCCESS;
  for (;;) {
    if (state.is_interrupted) {
      goto exitl;
    }
    size_t n = 0;
    ec = recycle(&n);
    if (!ec) {
      // Recycle with minimally adaptive frequency.
      if (!n) {
        thread_sleep(1000);
      } else if (n<=16) {
        quicktask_print_debug();
        thread_sleep(1);
      }
    } else if (QUICKTASK_MEMORY_ALLOCATION_FAILED==ec) {
      // We're the hero to return the stolen memory! But first lets wait
      // a bit to hopefully give us some space to swing the scythe.
      thread_sleep(10);
    } else {
      // return ec
      goto exitl;
    }
  }
exitl:
  lock(&State->mx);
  State->recycler_state = 0;
  unlock(&State->mx);
  printf("Recycler is closed\n");
  return ec;
}
LINUX_DUMMY_THREAD_FUN(recycle_worker)


// *** Spinlock functions
// **** check_null() (debug)
#ifdef QUICKTASK_DEBUG
static void check_null(Mutex*);
void check_null(Mutex *p) {
  if (!p) {
    // Had to remove informative message because the standard library's synchronization obscured a bug.
    //printf("Trying to lock zeroed pointer to mutex\n");
    abort();
  }
}
#endif
// **** try_lock()
int /*boolean*/ try_lock(Mutex *exclusion) {
#ifdef QUICKTASK_DEBUG
  check_null(exclusion);
#endif
  return atomic_try_lock(exclusion);
}
// **** lock()
void lock(Mutex *exclusion) {
#ifdef QUICKTASK_DEBUG
  check_null(exclusion);
#endif
  while (!atomic_try_lock(exclusion)) {
    cpu_pause();
    while (*exclusion)
      ;
  }
}
// **** unlock()
void unlock(Mutex *exclusion) {
#ifdef QUICKTASK_DEBUG
  check_null(exclusion);
#endif
  atomic_unlock(exclusion);
  // 0 == *exclusion
}

// *** State functions
// **** is_done()
int /*boolean*/ is_done(uint8_t x) {
  return STATE_DONE==x || (STATE_SPLIT | STATE_DONE)==x;
}
// **** is_open()
int is_open(uint8_t x) {
  return STATE_OPEN==x;
}
// **** is_split_open()
int is_split_open(uint8_t x) {
  return (STATE_SPLIT | STATE_OPEN)==x;
}
// **** is_split()
int is_split(uint8_t x) {
  return STATE_SPLIT==(x & ~0x0F);
}
// **** is_free()
int is_free(uint8_t x) {
  return STATE_FREE==x;
}
// **** is_split_arr()
void is_split_arr(QuickTask_Node *node, int out_bools[]) {
  out_bools[0] = is_split(node->state[0]);
  out_bools[1] = is_split(node->state[1]);
}
// **** check_has_open()
int /* boolean */ check_has_open(QuickTask_Node *node, size_t *out_ix) {
  if (is_open(node->state[0])) {
    *out_ix = 0;
    return 1;
  } else if (is_open(node->state[1])) {
    *out_ix = 1;
    return 1;
  }
  return 0;
}
// **** check_has_free()
int check_has_free(QuickTask_Node *node, size_t *out_ix) {
  volatile uint8_t *s = node->state;
  if (is_free(s[0])) {
    *out_ix = 0;
    return 1;
  } else if (is_free(s[1])) {
    *out_ix = 1;
    return 1;
  }
  return 0;
}
// **** check_has_split()
int check_has_split(QuickTask_Node *node, size_t *out_ix) {
  int sa [2];
  is_split_arr(node, sa);
  if (sa[0]) {
    *out_ix = 0;
    return 1;
  } else if (sa[1]) {
    *out_ix = 1;
    return 1;
  }
  return 0;
}
// **** check_has_split_open()
int check_has_split_open(QuickTask_Node *node, size_t *out_ix) {
  if (is_split_open(node->state[0])) {
    *out_ix = 0;
    return 1;
  } else if (is_split_open(node->state[1])) {
    *out_ix = 1;
    return 1;
  }
  return 0;
}
// *** The traversing functions
// **** try_descent_to_task()
// Moves deeper into the tree. Yes, the leaves are hanging down while the root
// is sticking up. It happens.
// The goal is to find an open task, which if found we'll assign to ourselves.
int try_descent_to_task(QuickTask_Node **inout_node, TaskDetail *out_td, size_t *out_ix) {
  QuickTask_Node *n = (QuickTask_Node*) *inout_node;
  size_t ix;
  for (;;) {
    //printf("Descending\n");
    lock(&n->mx);
    if (check_has_open(n, &ix)) {
      n->state[ix] = STATE_ASSIGNED;
      unlock(&n->mx);
      //printf("Took task (%p) [%zu]\n", (void*) n, ix);
      *inout_node = n;
      *out_td = n->tasks[ix];
      *out_ix = ix;
      return 1;
    } else if (check_has_split_open(n, &ix)) {
      QuickTask_Node *cn = (QuickTask_Node*) n->tasks[ix].q.child_node;
      lock(&cn->mx);
      ++(cn->ref_count);
      --(n->ref_count);
      unlock(&cn->mx);
      unlock(&n->mx);
      n = cn;
      *inout_node = n;
    } else {
      unlock(&n->mx);
      break;
    }
  }
  //printf("tdtt: no splits no open (at %p)\n", n);
  return 0;
}
// **** try_ascent_to_split()
// Moves up towards the root node in search of an open split, i.e. a branching
// point that should contain open tasks below. If we find a branching point, we
// return the first node _under_ the branching point. Note that finding an open
// split does not guarantee the existance of any open tasks.
int try_ascent_to_split(QuickTask_Node **inout_node) {
  QuickTask_Node *n = *inout_node;
  for (;;) {
    //printf("Ascending from %p\n", n);
    // No open split and no parent means we are at the top node and there's no
    // work; so we just exit right away. There's no need to lock to prevent
    // basing this decision on an inconsistent state since we'll get called
    // again anyway.
    size_t ix;
    if (!check_has_split_open(n, &ix) && !n->parent) {
      //printf("Took quick exit\n");
      return 0;
    }
    lock(&n->mx);
    if (!check_has_split_open(n, &ix)) {
      QuickTask_Node *p = (QuickTask_Node*) n->parent;
      unlock(&n->mx);
      if (p) {
        //printf("Going up to %p\n", p);
        lock(&p->mx);
        lock(&n->mx);
        ++(p->ref_count);
        --(n->ref_count);
        unlock(&n->mx);
        unlock(&p->mx);
        n = p;
        *inout_node = n;
      } else {
        //printf("No split found, top node.\n");
        return 0;
      }
    } else {
      //printf("(warning, inside locked region) tats: %p has split_open\n", n);
      QuickTask_Node *c = (QuickTask_Node*) n->tasks[ix].q.child_node;
      size_t cix;
      // If we find that all tasks or splits in the child are spoken for, we
      // report the state in the parent as split-assigned to prevent us or
      // other workers from unproductively following the split. Both parent and
      // child have to be locked while making this decision and assignment
      // because another worker might report the parent as split-done.
      lock(&c->mx);
      if ( !check_has_open(c, &cix) && !check_has_split_open(c, &cix) &&
           !are_all_done(c) ) {
        n->state[ix] = STATE_SPLIT | STATE_ASSIGNED;
        unlock(&c->mx);
        unlock(&n->mx);
        //printf("Set to split-assigned (%p)\n", n);
        *inout_node = n;
        return 0;
      }
      ++(c->ref_count);
      --(n->ref_count);
      unlock(&c->mx);
      unlock(&n->mx);
      *inout_node = c;
      return 1;
    } 
  }
}
// **** try_join()
int try_join(QuickTask_Node **inout_node, TaskDetail *out_td,
             size_t *out_ix, WorkerState *inout_ws) {
#ifdef QUICKTASK_DEBUG
  //printf("in %s from (%p) \n", __func__, (void*) *inout_node);
#endif
  for (;;) {
    //interruption_point(inout_ws);
    //printf("SPIN\n");
    if (try_descent_to_task(inout_node, out_td, out_ix)) {
      //printf("Found a task after descent\n");
      return 1;
    }
    if (!try_ascent_to_split(inout_node)) {
      //printf("Didn't find a task after descent\n");
      return 0;
    }
    thread_sleep(1);  // TODO(bas) Consider utility of sleep after not finding a good split in try_join()
                      //           Shouldn't the sleep happen before the return 0? Or not at all?
  }
}
// **** try_enter_tree()
int /* boolean */ try_enter_tree(QuickTask_Node **node) {
  lock(&RootH->mx);
  QuickTask_Node *n = (QuickTask_Node*) RootH->ptr;
  size_t i;
  if (!check_has_open(n, &i) && !check_has_split_open(n, &i)) {
    unlock(&RootH->mx);
#ifdef QUICKTASK_DEBUG
    printf("Couldn't enter tree: no work or split\n");
#endif
    return 0;
  }
  lock(&n->mx);
  ++(n->ref_count);
  unlock(&n->mx);
  unlock(&RootH->mx);
  *node = n;
  return 1;
}
// **** report_done()
void report_done(QuickTask_Node **inout_node, size_t ix) {
  //printf("in %s\n", __func__);
  //printf("Reporting task as done (%p) [%zu]\n", (void*) *inout_node, ix);
  QuickTask_Node *n = *inout_node;
  lock(&n->mx);
  n->state[ix] = STATE_DONE;
  size_t other_ix = !ix;
  uint8_t other_state = n->state[other_ix];
  QuickTask_Node *p = (QuickTask_Node*) n->parent;
  int8_t pix = n->parent_ix;
  if ( STATE_DONE!=other_state &&
      (STATE_SPLIT | STATE_DONE)!=other_state &&
       STATE_FREE!=other_state ) {
    unlock(&n->mx);
    //printf("Leaving report_done since other is not done or free.\n");
    return;
  } else {
    unlock(&n->mx);
    // What happens if another thread grows the tree and gives our node a parent
    // here? Well the other thread has to lock our node to do so and will find
    // its state. So, the other thread will set the new parent's state to SPLIT
    // or SPLIT | DONE for us.
    // TODO Check and double check the above, especially as it
    // relates to resource management.
    while (p) {
      //printf("moving up in tree from report done\n");

      lock(&p->mx);
      lock(&n->mx);
      if (p!=n->parent) {
        unlock(&n->mx);
        unlock(&p->mx);
        break;
      }
      ++(p->ref_count);
      --(n->ref_count);
      pix = n->parent_ix;
      if (1==p->ref_count && !n->ref_count) {
        p->state[pix] = STATE_DONE;
        release_node(n);
        unlock(&p->mx);
        //printf("Direct release of node %p\n", (void*)n);
      } else {
        p->state[pix] = STATE_SPLIT | STATE_DONE;
        unlock(&n->mx);
        unlock(&p->mx);
        //printf("Didn't release directly.\n");
      }

      n = p;
      ix = pix;
      lock(&n->mx);
      p = (QuickTask_Node*) n->parent;
      //printf("locked n\n");
      //n->state[ix] = STATE_DONE | STATE_SPLIT;
      other_ix = !ix;
      other_state = n->state[other_ix];
      pix = n->parent_ix;
      if (is_done(other_state) || is_free(other_state)) {
        unlock(&n->mx);
        //printf("unlocked p and reporting up (at %p for %p)\n", (void*) n, (void*) p);
        // continue reporting upwards
      } else if (is_open(other_state) ||
          (STATE_SPLIT | STATE_OPEN)==other_state) {
        unlock(&n->mx);
        *inout_node = n;
        //printf("rd() at open / split-open (%p)\n", (void*) n);
        return;
      } else {
        unlock(&n->mx);
        //printf("rd() at split-assigned/free\n");
        *inout_node = n;
        return;
      }
    }
    //printf("found top node in report\n");
    *inout_node = n;  // Top node found
    return;
  }
}
// **** report_error()
void report_error(QuickTask_Node **inout_node, size_t ix) {
  printf("No error reporting yet. Error in node (%p) [%zu]\n", (void*) *inout_node, ix);
  report_done(inout_node, ix);
}
// **** split_rec()
void split_rec(QuickTask_Node **node, TaskDetail *td, size_t *inout_ix) {
  size_t ix = *inout_ix;
  if ((*node)->state[ix]!=STATE_ASSIGNED) {
    printf("%s was called but shouldn't have been!\n", __func__);
    return;
  }
  QuickTask_TaskFun const fun = (*node)->tasks[ix].fun;
  QuickTask_SplitFun const split = (*node)->tasks[ix].split;
  TaskDetail *ptd = 0;  // To avoid copying the same TD twice.
  QuickTask_Range *r;
  size_t n_split;
  QuickTask_Node *n = *node;
  QuickTask_Node *cn = 0;
  QuickTask_Range rs [2];
  QuickTask_Node *const orig_n = n;
  for (;;) {
    r = (QuickTask_Range*) &(n->tasks[ix].q.range);
    split(r, &n_split, rs);
    if (2!=n_split) {
      //printf("Stopped splitting (ptd: %p) (td: %p)\n", ptd, td);
      if (ptd) {
        *td = *ptd;
      }
      if (orig_n!=n) {
        lock(&n->mx);
        ++(n->ref_count);
        unlock(&n->mx);
        lock(&orig_n->mx);
        --(orig_n->ref_count);
        unlock(&orig_n->mx);
      }
      return;
    }
    int ec = take_free_node(&cn);
    if (ec) {
      // TODO IMPORTANT Should return ec
      return;
    }
    //printf("Splitting task %p [%zu] -> %p\n", (void*) n, ix, (void*) cn);
    init_node(cn);
    set_task(cn, 0, fun, split, rs[0]);
    set_task(cn, 1, fun, split, rs[1]);
    cn->state[0] = STATE_ASSIGNED;
    cn->parent = n;
    cn->parent_ix = ix;

    lock(&n->mx);
    //lock(&cn->mx);
    //printf("SPLIT_REC state before: %d\n", n->state[ix]);
    if (STATE_ASSIGNED!=n->state[ix]) {
      printf("OH NO\n");
      unlock(&n->mx);
      //unlock(&cn->mx);
      abort();
      return;
    }
    //++(cn->ref_count);
    set_child(n, ix, cn);
    //print_set_child_debug(n, cn);

    //unlock(&cn->mx);
    unlock(&n->mx);

    ix = 0;
    n = cn;
    *node = n;
    ptd = (TaskDetail*) &(n->tasks[ix]);
    *inout_ix = ix;
  }
}

// *** Worker functions
// **** worker()
unsigned int THREADCALL worker(void *dummy) {
  WorkerState state;
  printf("started worker\n");
  init_worker_state(&state);
  QuickTask_Node *n = 0;
  TaskDetail td;
  size_t ix;
  for (;;) {
    interruption_point(&state);
    if (!try_enter_tree(&n)) {
      thread_sleep(100);
    } else {
      for (;;) {
        interruption_point(&state);
        int have_task = try_join(&n, &td, &ix, &state);
        while (!have_task) {
          thread_sleep(10);
          interruption_point(&state);
          have_task = try_join(&n, &td, &ix, &state);
        }
        split_rec(&n, &td, &ix);
        int ec = do_work(n, &td, ix);
        if (!ec) {
          report_done(&n, ix);
        } else {
          report_error(&n, ix);
        }
      }
    }
  }
  // We won't get here; see interruption_point() for thread exit.
  return 0;
}
LINUX_DUMMY_THREAD_FUN(worker)
// **** interruption_point()
void interruption_point(WorkerState volatile *state) {
  if (!state->is_interrupted) {
    return;
  } else {
#ifdef QUICKTASK_DEBUG
    printf("Interruption request received by worker (state %p)\n", (void*) state);
#endif
    deinit_worker_state((WorkerState*)state);
    end_thread(0);
  }
}
// **** do_work()
int do_work(QuickTask_Node *n, TaskDetail *td, size_t ix) {
  //printf("running task: %p [%zu]\n", (void*) n, ix);
  QuickTask_TaskFun const fun = td->fun;
  QuickTask_Range const r = td->q.range;
  int ec = fun(r);
  return ec;
}
// **** init_worker_state()
void init_worker_state(WorkerState *state) {
  state->is_interrupted = 0;
  lock(&State->mx);
  size_t const my_ix = State->num_workers;
  state->my_index = my_ix;
  ++(State->num_workers);
  State->worker_states[my_ix] = state;
  //printf("Num workers: %d\n", State->num_workers);
  unlock(&State->mx);
}
// **** deinit_worker_state()
void deinit_worker_state(WorkerState *state) {
  lock(&State->mx);
  State->worker_states[state->my_index] = 0;
  unlock(&State->mx);
}
// *** init_node()
void init_node(QuickTask_Node *n) {
  TaskDetail const t_zero = { .q.range = {.first = 0, .end = 0} };
  QuickTask_Node const zero = {
    .state = { STATE_FREE, STATE_FREE },
    .tasks = { t_zero, t_zero },
    .parent = 0
    // ... rely on automatic zero-initialization
  };
  *n = zero;
}
// *** copy_tasks()
void copy_tasks(QuickTask_Node *node, TaskDetail tds[]) {
  tds[0] = node->tasks[0];
  tds[1] = node->tasks[1];
}
// *** set_task()
void set_task(QuickTask_Node *n, size_t ix,
              QuickTask_TaskFun fun,
              QuickTask_SplitFun split,
              QuickTask_Range r) {
  n->state[ix] = STATE_OPEN;
  //printf("setting task fun to %p on %p\n", fun, n);
  n->tasks[ix].fun = fun;
  n->tasks[ix].split = split;
  n->tasks[ix].q.range = r;
}
// *** set_child()
// CAUTION Modifies only parent. So it does not set the child's parent or parent_ix!
// Reason: leave locking to caller.
void set_child(QuickTask_Node *parent, size_t ix, QuickTask_Node const*child) {
  if (parent!=child) {
    parent->state[ix] = STATE_SPLIT | STATE_OPEN;
    parent->tasks[ix].q.child_node = (QuickTask_Node*) child;
  }
}
// *** print_set_child_debug()
void print_set_child_debug(QuickTask_Node const *p, QuickTask_Node const *c) {
  printf("setting parent->child: %p, [?], child: %p\n", (void*) p, (void*) c);
  print_node(p);
  print_node(c);
  printf("\n");
}
// *** take_free_node()
// TODO(bas) take_free_node() and release_node() are stubs; there is no free-list.
int take_free_node(QuickTask_Node **node) {
  QuickTask_Node *n = cla_malloc(sizeof(*n));
  if (!n) {
    return QUICKTASK_MEMORY_ALLOCATION_FAILED;
  }
  *node = n;
  return QUICKTASK_SUCCESS;
}
// *** release_node()
void release_node(QuickTask_Node *node) {
  if (node) {
    node->parent = 0;
    node->tasks[0].q.child_node = 0;
    node->tasks[1].q.child_node = 0;
    cla_free((void*)node);
  }
}
// *** quicktask_init()
int quicktask_init() {
  int ret = QUICKTASK_SUCCESS;
  printf("sizeof(Node):%zu\n", sizeof(QuickTask_Node));
  printf("sizeof(TaskDetail):%zu\n", sizeof(TaskDetail));
  full_memory_barrier();
  //thread_sleep(1000);
  if (State) {
    printf("Quicktask already initialized.\n");
    return QUICKTASK_ALREADY_INITIALIZED;
  }
  State = cla_malloc(sizeof(*State));
  if (!State) {
    ret = QUICKTASK_MEMORY_ALLOCATION_FAILED;
    goto mem_failed;
  }
  State->thread_handles = cla_malloc(sizeof(State->thread_handles[0])*NUM_THREADS);
  if (!State->thread_handles) {
    ret = QUICKTASK_MEMORY_ALLOCATION_FAILED;
    goto mem_failed_free_state;
  }
  for (size_t i = 0; i!=NUM_THREADS; ++i) {
    State->thread_handles[i] = 0;
  }
  State->worker_states = cla_malloc(sizeof(State->worker_states[0])*NUM_THREADS);
  if (!State->worker_states) {
    ret = QUICKTASK_MEMORY_ALLOCATION_FAILED;
    goto mem_failed_free_thread_handles;
  }
  RootH = cla_malloc(sizeof(*RootH));
  if (!RootH) {
    ret = QUICKTASK_MEMORY_ALLOCATION_FAILED;
    goto mem_failed_free_worker_states;
  }
  QuickTask_Node *root = cla_malloc(sizeof(*root));
  if (!root) {
    ret = QUICKTASK_MEMORY_ALLOCATION_FAILED;
    goto mem_failed_free_RootH;
  }
  printf("About to init root node.\n");
  init_node(root);
  RootH->ptr = root;
  ThreadHandle hs [NUM_THREADS];
  printf("About to init threads.\n");
  for (size_t i = 0; i<NUM_THREADS; ++i) {
    if (begin_thread(WORKER_FUN(worker), &hs[i])) {
      hs[i] = 0;
    }
  }
  lock(&State->mx);
  for (size_t i = 0; i<NUM_THREADS; ++i) {
    if (!hs[i]) {
      unlock(&State->mx);
      ret = QUICKTASK_WORKER_THREAD_CREATION_FAILED;
      goto thread_failed;
    }
    State->thread_handles[i] = hs[i];
  }
  unlock(&State->mx);

  ThreadHandle rh;
  if (begin_thread(WORKER_FUN(recycle_worker), &rh)) {
    rh = 0;
    goto recycler_failed;
  }
  lock(&State->mx);
  State->recycler_handle = rh;
  unlock(&State->mx);

  return QUICKTASK_SUCCESS;

recycler_failed:
  printf("Recycler thread failed.\n");
thread_failed:
  printf("About to interrupt workers.\n");
  interrupt_workers();  
mem_failed_free_RootH:
  printf("About to free RootH.\n");
  cla_free((RootHolder*) RootH);
  RootH = 0;
mem_failed_free_worker_states:
  printf("About to free worker states.\n");
  cla_free(State->worker_states);
  State->worker_states = 0;
mem_failed_free_thread_handles:
  printf("About to free thread handles.\n");
  cla_free(State->thread_handles);
  State->thread_handles = 0;
mem_failed_free_state:
  printf("About to free State.\n");
  cla_free((ThreadMgrState*)State);
  State = 0;
mem_failed:
  return ret;
}
// *** quicktask_deinit()
int quicktask_deinit() {
  if (!State || !RootH) {
    return QUICKTASK_SUCCESS;
  }
  printf("Deinitializing ... root node is (%p)\n", (void*) RootH->ptr);
  // kill threads
  interrupt_workers();

  interrupt_recycler();

  // TODO recursize free of RootH

  lock(&State->mx);
  State->recycler_handle = 0;
  cla_free((RootHolder*)RootH);
  RootH = 0;
  cla_free((ThreadMgrState*)State);
  State = 0;
  return QUICKTASK_SUCCESS;
}
// *** interrupt_workers()
void interrupt_workers() {
  lock(&State->mx);
  size_t const num_threads = State->num_workers;
  WorkerState volatile **ws_ptrs = State->worker_states;
  for (size_t i = 0; i!=num_threads; ++i) {
    WorkerState volatile *ws = ws_ptrs[i];
    if (ws) {
      //printf("Interrupting ws %p\n", ws);
      if (!ws->is_interrupted) {
        int unused = atomic_try_lock(&ws->is_interrupted);  // TODO change to atomic_inc()
        ++unused;  // Compiler happiness. Remove when changed.
      }
    }
  }
  unlock(&State->mx);

  for (size_t i = 0; i!=num_threads; ++i) {
    WorkerState volatile *ws;
    do {
      lock(&State->mx);
      full_memory_barrier();
      ws = ws_ptrs[i];
      if (!ws) {
        if (State->thread_handles[i]) {
          release_thread(State->thread_handles[i]);
          State->thread_handles[i] = 0;
        }
        unlock(&State->mx);
        printf("Thread %zu is closed\n", i);
        break;
      }
      unlock(&State->mx);
      thread_sleep(10);
    } while (ws);
  }
}
// *** interrupt_recycler()
void interrupt_recycler() {
  lock(&State->mx);
  if (!State->recycler_state) {
    return;
  }
  int unused = try_lock(&State->recycler_state->is_interrupted); ++unused;
  unlock(&State->mx);
  while (State->recycler_state) {
    thread_sleep(1);
  }
  release_thread(State->recycler_handle);
  lock(&State->mx);
  State->recycler_handle = 0;
  unlock(&State->mx);
}
// *** quicktask_is_done()
int quicktask_is_done(QuickTask_Node *node) {
  return are_all_done(node);
}
// *** quicktask_wait_noncoop()
int quicktask_wait_noncoop(QuickTask_Node volatile *node) {
  if (!node) {
    //printf("%s called with null ptr\n", __func__);
  }
  for (;;) {
    while (!are_all_done(node)) {
      thread_sleep(1);
    }
    lock(&node->mx);
    if (are_all_done(node)) {
      --(node->ref_count);
      unlock(&node->mx);
      int ec = node->user_error[0] ? node->user_error[0] : node->user_error[1];  // TODO Check case when task wasn't split at top level
      //size_t unused;
      //recycle(&unused);
      return ec;
      //return QUICKTASK_SUCCESS;
    }
    unlock(&node->mx);
  }
}
// *** are_all_done()
int are_all_done(QuickTask_Node volatile *n) {
  uint8_t const s0 = n->state[0];
  uint8_t const s1 = n->state[1];
  //printf("aad? %d and %d\n", s0, s1);
  return (is_done(s0) || is_free(s0)) &&
         (is_done(s1) || is_free(s1));
}
// *** quicktask_wait_coop()
int quicktask_wait_coop(QuickTask_Node volatile *node) {
  return 1;
}
// *** quicktask_insert_task()
int quicktask_insert_task(QuickTask_TaskFun fun,
                          QuickTask_SplitFun split,
                          QuickTask_Range r,
                          QuickTask_Node **out_node) {
  if (!fun || !split || !out_node) {
    return QUICKTASK_INVALID_ARG;
  }
  // Prepare a fresh node with the task.
  QuickTask_Node *cn = 0;  // make compiler happy
  int ec = take_free_node(&cn);
  if (ec) {
    *out_node = 0;
    return ec;
  }
  init_node(cn);
  cn->ref_count = 1;
  size_t n;
  QuickTask_Range rs[2];
  split(&r, &n, rs);
  if (2==n) {
    set_task(cn, 0, fun, split, rs[0]);
    set_task(cn, 1, fun, split, rs[1]);
  } else {
    set_task(cn, 0, fun, split, r);
  }
  size_t i;
  lock(&RootH->mx);
  QuickTask_Node *root = (QuickTask_Node*) RootH->ptr;
  //printf("root ptr: %p\n", root);
  lock(&root->mx);
  if (check_has_free(root, &i)) {
    set_child(root, i, cn);
    cn->parent_ix = i;
    cn->parent = root;
    //print_set_child_debug(root, cn);
    unlock(&root->mx);
    unlock(&RootH->mx);
    *out_node = cn;
    return QUICKTASK_SUCCESS;
  } else {
    QuickTask_Node *node;
    ec = take_free_node(&node);  // Should use loop with double check instead
    if (ec) {
      unlock(&root->mx);
      unlock(&RootH->mx);
      *out_node = 0;
      return ec;
    }
    set_child(node, 0, root);
    set_child(node, 1, cn);
    cn->parent_ix = 1;
    cn->parent = node;
    root->parent_ix = 0;
    root->parent = node;
    //print_set_child_debug(node, root);
    //print_set_child_debug(node, cn);
    unlock(&root->mx);
    RootH->ptr = node;
    unlock(&RootH->mx);
    *out_node = cn;
    return QUICKTASK_SUCCESS;
  }
}
// *** quicktask_print_debug()
void quicktask_print_debug() {
  printf("Debug Message\n");
  QuickTask_Node *n = (QuickTask_Node*) RootH->ptr;
  print_node(n);  // 0
  for (size_t i = 0; i!=2; ++i) {  // 1
    if (is_split(n->state[i])) {
      QuickTask_Node *c = (QuickTask_Node*) n->tasks[i].q.child_node;
      print_node(c);
      for (size_t j = 0; j!=2; ++j) {  // 2
        if (is_split(c->state[j])) {
          QuickTask_Node *cc = (QuickTask_Node*) c->tasks[j].q.child_node;
          print_node(cc);
          for (size_t k = 0; k!=2; ++k) {  // 3
            if (is_split(cc->state[k])) {
              QuickTask_Node *ccc = (QuickTask_Node*) cc->tasks[k].q.child_node;
              print_node(ccc);
              for (size_t l = 0; l!=2; ++l) {  // 4
                if (is_split(ccc->state[l])) {
                  QuickTask_Node *cccc = (QuickTask_Node*) ccc->tasks[l].q.child_node;
                  print_node(cccc);
                  for (size_t m = 0; m!=2; ++m) {  // 5
                    if (is_split(cccc->state[m])) {
                      QuickTask_Node *ccccc = (QuickTask_Node*) cccc->tasks[m].q.child_node;
                      print_node(ccccc);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  } 
  printf("End Debug Message\n\n");
}
// *** state_string()
static char const* state_string(uint8_t);
char const* state_string(uint8_t s) {
  switch (s) {
  case STATE_FREE: return "Free";
  case STATE_OPEN: return "Open";
  case STATE_ASSIGNED: return "Assigned";
  case STATE_DONE: return "Done";
  case STATE_SPLIT | STATE_FREE: return "SPLIT-Free";
  case STATE_SPLIT | STATE_OPEN: return "SPLIT-Open";
  case STATE_SPLIT | STATE_ASSIGNED: return "SPLIT-Assigned";
  case STATE_SPLIT | STATE_DONE: return "SPLIT-Done";
  default:
    return "Couldn't recognize state";
  }
}
// *** print_node()
void print_node(QuickTask_Node const *node) {
  printf("--Node %p--\n", (void*) node);
  printf("  mutex: %d\n", node->mx);
  printf("  parent_ix: %d\n", node->parent_ix);
  printf("  state[0]: %s\n", state_string(node->state[0]));
  printf("  state[1]: %s\n", state_string(node->state[1]));
  printf("  ref_count: %d\n", node->ref_count);
  printf("  user_error: %d %d\n", (int) node->user_error[0], (int) node->user_error[1]);
  printf("  parent: %p\n", (void*) node->parent);
  printf("  tasks[0].fun: %p\n", (void*)(size_t) node->tasks[0].fun);
  printf("  [0].q.first, end: %d, %d <or> %p\n", node->tasks[0].q.range.first
                                               , node->tasks[0].q.range.end
                                               , (void*) node->tasks[0].q.child_node);
  printf("  tasks[1].fun: %p\n", (void*)(size_t) node->tasks[1].fun);
  printf("  [1].q.first, end: %d, %d <or> %p\n", node->tasks[1].q.range.first
                                               , node->tasks[1].q.range.end
                                               , (void*) node->tasks[1].q.child_node);
  printf("\n");
}

#ifdef __cplusplus
}
#endif
