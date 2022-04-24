/*****************************************************************************

Copyright (c) 2015, 2018, Oracle and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/ut0lf_hash.h
 Lock free hash implementation (ported and modified from sql/lf_hash.cc)

 Created Aug 23, 2021 rongbiao xie(guimo.xrb)
 *******************************************************/

#ifndef ut0lf_hash
#define ut0lf_hash

#include <iostream>
#include <assert.h>
#include <stddef.h>
#include <atomic>
#include <algorithm>
#include <sys/types.h>
#include <string.h>

typedef unsigned char uchar;
typedef uint32_t uint32;
typedef int32_t int32;
typedef intptr_t intptr;
typedef unsigned long int ulint;
#define INT_MAX32       0x7FFFFFFFL
#define DBUG_ASSERT(A) assert(A)

#define lf_free(X) free(X)
#define lf_alloc(X) malloc(X)
#define lf_max(a,b) ((a) > (b) ? (a) : (b))
#define lf_thread_yield sched_yield()

const uchar bits_reverse_table[256] = {
    0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0,
    0x30, 0xB0, 0x70, 0xF0, 0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8,
    0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8, 0x04, 0x84, 0x44, 0xC4,
    0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4,
    0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC,
    0x3C, 0xBC, 0x7C, 0xFC, 0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2,
    0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2, 0x0A, 0x8A, 0x4A, 0xCA,
    0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
    0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6,
    0x36, 0xB6, 0x76, 0xF6, 0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE,
    0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE, 0x01, 0x81, 0x41, 0xC1,
    0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
    0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9,
    0x39, 0xB9, 0x79, 0xF9, 0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5,
    0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5, 0x0D, 0x8D, 0x4D, 0xCD,
    0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
    0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3,
    0x33, 0xB3, 0x73, 0xF3, 0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB,
    0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB, 0x07, 0x87, 0x47, 0xC7,
    0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
    0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF,
    0x3F, 0xBF, 0x7F, 0xFF};

static inline uint32 reverse_bits(uint32 key) {
  return (bits_reverse_table[key & 255] << 24) |
         (bits_reverse_table[(key >> 8) & 255] << 16) |
         (bits_reverse_table[(key >> 16) & 255] << 8) |
         bits_reverse_table[(key >> 24)];
}

/* clear the highest bit of v */
static inline uint32 clear_highest_bit(uint32 v) {
  uint32 w = v >> 1;
  w |= w >> 1;
  w |= w >> 2;
  w |= w >> 4;
  w |= w >> 8;
  w |= w >> 16;
  return v & w;
}

#if defined(__cplusplus)
inline bool likely(bool expr) { return __builtin_expect(expr, true); }
inline bool unlikely(bool expr) { return __builtin_expect(expr, false); }
#else
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)
#endif

#define LF_BACKOFF (1)

/* 
  wait-free dynamic array
*/
#define LF_DYNARRAY_LEVEL_LENGTH 256
#define LF_DYNARRAY_LEVELS 4

typedef struct {
  std::atomic<void *> level[LF_DYNARRAY_LEVELS];
  uint size_of_element;
} LF_DYNARRAY;

typedef int (*lf_dynarray_func)(void *, void *);

void lf_dynarray_init(LF_DYNARRAY *array, uint element_size) {
  std::fill(array->level, array->level + LF_DYNARRAY_LEVELS, nullptr);
  array->size_of_element = element_size;
}

static void recursive_free(std::atomic<void *> *alloc, int level) {
  if (!alloc) {
    return;
  }

  if (level) {
    int i;
    for (i = 0; i < LF_DYNARRAY_LEVEL_LENGTH; i++)
      recursive_free(static_cast<std::atomic<void *> *>(alloc[i].load()),
                     level - 1);
    lf_free(alloc);
  } else {
    lf_free(alloc[-1]);
  }
}

void lf_dynarray_destroy(LF_DYNARRAY *array) {
  int i;
  for (i = 0; i < LF_DYNARRAY_LEVELS; i++)
    recursive_free(static_cast<std::atomic<void *> *>(array->level[i].load()),
                   i);
}

static const ulong dynarray_idxes_in_prev_levels[LF_DYNARRAY_LEVELS] = {
    0, /* +1 here to to avoid -1's below */
    LF_DYNARRAY_LEVEL_LENGTH,
    LF_DYNARRAY_LEVEL_LENGTH *LF_DYNARRAY_LEVEL_LENGTH +
        LF_DYNARRAY_LEVEL_LENGTH,
    LF_DYNARRAY_LEVEL_LENGTH *LF_DYNARRAY_LEVEL_LENGTH
            *LF_DYNARRAY_LEVEL_LENGTH +
        LF_DYNARRAY_LEVEL_LENGTH *LF_DYNARRAY_LEVEL_LENGTH +
        LF_DYNARRAY_LEVEL_LENGTH};

static const ulong dynarray_idxes_in_prev_level[LF_DYNARRAY_LEVELS] = {
    0, /* +1 here to to avoid -1's below */
    LF_DYNARRAY_LEVEL_LENGTH,
    LF_DYNARRAY_LEVEL_LENGTH *LF_DYNARRAY_LEVEL_LENGTH,
    LF_DYNARRAY_LEVEL_LENGTH *LF_DYNARRAY_LEVEL_LENGTH
        *LF_DYNARRAY_LEVEL_LENGTH,
};

/*
  Returns a valid lvalue pointer to the element number 'idx'.
  Allocates memory if necessary.
*/
void *lf_dynarray_lvalue(LF_DYNARRAY *array, uint idx) {
  void *ptr;
  int i;

  for (i = LF_DYNARRAY_LEVELS - 1; idx < dynarray_idxes_in_prev_levels[i]; i--)
    /* no-op */;
  std::atomic<void *> *ptr_ptr = &array->level[i];
  idx -= dynarray_idxes_in_prev_levels[i];
  for (; i > 0; i--) {
    if (!(ptr = *ptr_ptr)) {
      void *alloc = lf_alloc(LF_DYNARRAY_LEVEL_LENGTH * sizeof(void *));
      if (unlikely(!alloc)) {
        return (NULL);
      }
      if (atomic_compare_exchange_strong(ptr_ptr, &ptr, alloc)) {
        ptr = alloc;
      } else {
        lf_free(alloc);
      }
    }
    ptr_ptr =
        ((std::atomic<void *> *)ptr) + idx / dynarray_idxes_in_prev_level[i];
    idx %= dynarray_idxes_in_prev_level[i];
  }
  if (!(ptr = *ptr_ptr)) {
    uchar *alloc, *data;
    alloc = static_cast<uchar *>(
        lf_alloc(LF_DYNARRAY_LEVEL_LENGTH * array->size_of_element +
                      lf_max(array->size_of_element, sizeof(void *))));
    if (unlikely(!alloc)) {
      return (NULL);
    }
    /* reserve the space for free() address */
    data = alloc + sizeof(void *);
    {
      /* alignment */
      intptr mod = ((intptr)data) % array->size_of_element;
      if (mod) {
        data += array->size_of_element - mod;
      }
    }
    ((void **)data)[-1] = alloc; /* free() will need the original pointer */
    if (atomic_compare_exchange_strong(ptr_ptr, &ptr,
                                       static_cast<void *>(data))) {
      ptr = data;
    } else {
      lf_free(alloc);
    }
  }
  return ((uchar *)ptr) + array->size_of_element * idx;
}

/*
  Returns a pointer to the element number 'idx'
  or NULL if an element does not exists
*/
void *lf_dynarray_value(LF_DYNARRAY *array, uint idx) {
  void *ptr;
  int i;

  for (i = LF_DYNARRAY_LEVELS - 1; idx < dynarray_idxes_in_prev_levels[i]; i--)
    /* no-op */;
  std::atomic<void *> *ptr_ptr = &array->level[i];
  idx -= dynarray_idxes_in_prev_levels[i];
  for (; i > 0; i--) {
    if (!(ptr = *ptr_ptr)) {
      return (NULL);
    }
    ptr_ptr =
        ((std::atomic<void *> *)ptr) + idx / dynarray_idxes_in_prev_level[i];
    idx %= dynarray_idxes_in_prev_level[i];
  }
  if (!(ptr = *ptr_ptr)) {
    return (NULL);
  }
  return ((uchar *)ptr) + array->size_of_element * idx;
}

static int recursive_iterate(LF_DYNARRAY *array, void *ptr, int level,
                             lf_dynarray_func func, void *arg) {
  int res, i;
  if (!ptr) {
    return 0;
  }
  if (!level) {
    return func(ptr, arg);
  }
  for (i = 0; i < LF_DYNARRAY_LEVEL_LENGTH; i++)
    if ((res = recursive_iterate(array, ((void **)ptr)[i], level - 1, func,
                                 arg))) {
      return res;
    }
  return 0;
}

/*
  Calls func(array, arg) on every array of LF_DYNARRAY_LEVEL_LENGTH elements
  in lf_dynarray.

  DESCRIPTION
    lf_dynarray consists of a set of arrays, LF_DYNARRAY_LEVEL_LENGTH elements
    each. lf_dynarray_iterate() calls user-supplied function on every array
    from the set. It is the fastest way to scan the array, faster than
      for (i=0; i < N; i++) { func(lf_dynarray_value(dynarray, i)); }

  NOTE
    if func() returns non-zero, the scan is aborted
*/
int lf_dynarray_iterate(LF_DYNARRAY *array, lf_dynarray_func func, void *arg) {
  int i, res;
  for (i = 0; i < LF_DYNARRAY_LEVELS; i++)
    if ((res = recursive_iterate(array, array->level[i], i, func, arg))) {
      return res;
    }
  return 0;
}


/*
  pin manager for memory allocator
*/
#define LF_PINBOX_PINS 4
#define LF_PURGATORY_SIZE 100
#define LF_PINBOX_MAX_PINS 65536

typedef void lf_pinbox_free_func(void *, void *, void *);

typedef struct {
  LF_DYNARRAY pinarray;
  lf_pinbox_free_func *free_func;
  void *free_func_arg;
  uint free_ptr_offset;
  std::atomic<uint32> pinstack_top_ver; /* this is a versioned pointer */
  std::atomic<uint32> pins_in_array;    /* number of elements in array */
} LF_PINBOX;

struct LF_PINS {
  std::atomic<void *> pin[LF_PINBOX_PINS];
  LF_PINBOX *pinbox;
  void *purgatory;
  uint32 purgatory_count;
  std::atomic<uint32> link;
  /* we want sizeof(LF_PINS) to be 64 to avoid false sharing */
#if SIZEOF_INT * 2 + SIZEOF_CHARP * (LF_PINBOX_PINS + 2) != 64
  char pad[64 - sizeof(uint32) * 2 - sizeof(void *) * (LF_PINBOX_PINS + 2)];
#endif
};

/*
  Initialize a pinbox. Normally called from lf_alloc_init.
  See the latter for details.
*/
void lf_pinbox_init(LF_PINBOX *pinbox, uint free_ptr_offset,
                    lf_pinbox_free_func *free_func, void *free_func_arg) {
  DBUG_ASSERT(free_ptr_offset % sizeof(void *) == 0);
  static_assert(sizeof(LF_PINS) == 64, "");
  lf_dynarray_init(&pinbox->pinarray, sizeof(LF_PINS));
  pinbox->pinstack_top_ver = 0;
  pinbox->pins_in_array = 0;
  pinbox->free_ptr_offset = free_ptr_offset;
  pinbox->free_func = free_func;
  pinbox->free_func_arg = free_func_arg;
}

void lf_pinbox_destroy(LF_PINBOX *pinbox) {
  lf_dynarray_destroy(&pinbox->pinarray);
}

/*
  Get the next pointer in the purgatory list.
  Note that next_node is not used to avoid the extra volatile.
*/
#define pnext_node(P, X) (*((void **)(((char *)(X)) + (P)->free_ptr_offset)))

struct st_match_and_save_arg {
  LF_PINS *pins;
  LF_PINBOX *pinbox;
  void *old_purgatory;
};

static inline void add_to_purgatory(LF_PINS *pins, void *addr) {
  pnext_node(pins->pinbox, addr) = pins->purgatory;
  pins->purgatory = addr;
  pins->purgatory_count++;
}

/*
  Callback for lf_dynarray_iterate:
  Scan all pins of all threads, for each active (non-null) pin,
  scan the current thread's purgatory. If present there, move it
  to a new purgatory. At the end, the old purgatory will contain
  pointers not pinned by any thread.
*/
static int match_and_save(void *v_el, void *v_arg) {
  LF_PINS *el = static_cast<LF_PINS *>(v_el);
  st_match_and_save_arg *arg = static_cast<st_match_and_save_arg *>(v_arg);
  int i;
  LF_PINS *el_end = el + LF_DYNARRAY_LEVEL_LENGTH;
  for (; el < el_end; el++) {
    for (i = 0; i < LF_PINBOX_PINS; i++) {
      void *p = el->pin[i];
      if (p) {
        void *cur = arg->old_purgatory;
        void **list_prev = &arg->old_purgatory;
        // void *prev = cur;
        while (cur) {
          void *next = pnext_node(arg->pinbox, cur);

          /* Problem: the old_purgatory list can not be connect */
          if (p == cur) {
            /* pinned - keeping */
            add_to_purgatory(arg->pins, cur);
            /* unlink from old purgatory */
            /*
              if(cur == arg->old_purgatory){
                  *list_prev = next;
              }else{
                  pnext_node(pins->pinbox, prev) = next;
              }
            */
            *list_prev = next;
          } else {
            
            list_prev = (void **)((char *)cur + arg->pinbox->free_ptr_offset);
          }
          // prev = cur;
          cur = next;
        }
        if (!arg->old_purgatory) {
          return 1;
        }
      }
    }
  }
  return 0;
}

/*
  Scan the purgatory and free everything that can be freed
*/
static void lf_pinbox_real_free(LF_PINS *pins) {
  LF_PINBOX *pinbox = pins->pinbox;

  /* Store info about current purgatory. */
  struct st_match_and_save_arg arg = {pins, pinbox, pins->purgatory};
  /* Reset purgatory. */
  pins->purgatory = NULL;
  pins->purgatory_count = 0;

  lf_dynarray_iterate(&pinbox->pinarray, match_and_save, &arg);

  if (arg.old_purgatory) {
    /* Some objects in the old purgatory were not pinned, free them. */
    void *last = arg.old_purgatory;
    while (pnext_node(pinbox, last)) {
      last = pnext_node(pinbox, last);
    }
    pinbox->free_func(arg.old_purgatory, last, pinbox->free_func_arg);
  }
}

/*
  Get pins from a pinbox.

  SYNOPSYS
    pinbox      -

  DESCRIPTION
    get a new LF_PINS structure from a stack of unused pins,
    or allocate a new one out of dynarray.
*/
LF_PINS *lf_pinbox_get_pins(LF_PINBOX *pinbox) {
  uint32 pins, next, top_ver;
  LF_PINS *el;
  
  top_ver = pinbox->pinstack_top_ver;
  do {
    if (!(pins = top_ver % LF_PINBOX_MAX_PINS)) {
      /* the stack of free elements is empty */
      pins = pinbox->pins_in_array.fetch_add(1) + 1;
      if (unlikely(pins >= LF_PINBOX_MAX_PINS)) {
        return 0;
      }
      /*
        note that the first allocated element has index 1 (pins==1).
        index 0 is reserved to mean "NULL pointer"
      */
      el = (LF_PINS *)lf_dynarray_lvalue(&pinbox->pinarray, pins);
      if (unlikely(!el)) {
        return 0;
      }
      break;
    }
    el = (LF_PINS *)lf_dynarray_value(&pinbox->pinarray, pins);
    next = el->link;
  } while (!atomic_compare_exchange_strong(
      &pinbox->pinstack_top_ver, &top_ver,
      top_ver - pins + next + LF_PINBOX_MAX_PINS));
  /*
    set el->link to the index of el in the dynarray (el->link has two usages:
    - if element is allocated, it's its own index
    - if element is free, it's its next element in the free stack
  */
  el->link = pins;
  el->purgatory_count = 0;
  el->pinbox = pinbox;
  return el;
}

/*
  Put pins back to a pinbox.

  DESCRIPTION
    empty the purgatory (XXX deadlock warning below!),
    push LF_PINS structure to a stack
*/
void lf_pinbox_put_pins(LF_PINS *pins) {
  LF_PINBOX *pinbox = pins->pinbox;
  uint32 top_ver, nr;
  nr = pins->link;

  /*
    XXX this will deadlock if other threads will wait for
    the caller to do something after _lf_pinbox_put_pins(),
    and they would have pinned addresses that the caller wants to free.
    Thus: only free pins when all work is done and nobody can wait for you!!!
  */
  while (pins->purgatory_count) {
    lf_pinbox_real_free(pins);
    if (pins->purgatory_count) {
      lf_thread_yield;
    }
  }
  top_ver = pinbox->pinstack_top_ver;
  do {
    pins->link = top_ver % LF_PINBOX_MAX_PINS;
  } while (!atomic_compare_exchange_strong(
      &pinbox->pinstack_top_ver, &top_ver,
      top_ver - pins->link + nr + LF_PINBOX_MAX_PINS));
}

/*
  Free an object allocated via pinbox allocator

  DESCRIPTION
    add an object to purgatory. if necessary, call lf_pinbox_real_free()
    to actually free something.
*/
void lf_pinbox_free(LF_PINS *pins, void *addr) {
  add_to_purgatory(pins, addr);
  if (pins->purgatory_count % LF_PURGATORY_SIZE == 0) {
    lf_pinbox_real_free(pins);
  }
}

static inline void lf_pin(LF_PINS *pins, int pin, void *addr) {
#if defined(__GNUC__) && defined(MY_LF_EXTRA_DEBUG)
  assert(pin < LF_NUM_PINS_IN_THIS_FILE);
#endif
  pins->pin[pin].store(addr);
}

static inline void lf_unpin(LF_PINS *pins, int pin) {
#if defined(__GNUC__) && defined(MY_LF_EXTRA_DEBUG)
  assert(pin < LF_NUM_PINS_IN_THIS_FILE);
#endif
  pins->pin[pin].store(nullptr);
}

static inline std::atomic<uchar *> &next_node(LF_PINBOX *P, uchar *X) {
  std::atomic<uchar *> *free_ptr =
      (std::atomic<uchar *> *)(X + P->free_ptr_offset);
  return *free_ptr;
}

/*
  memory allocator
*/
#define anext_node(X) next_node(&allocator->pinbox, (X))

typedef void lf_allocator_func(uchar *);

struct LF_ALLOCATOR {
  LF_PINBOX pinbox;
  std::atomic<uchar *> top;
  uint element_size;
  std::atomic<uint32> mallocs;
  lf_allocator_func *constructor; /* called, when an object is malloc()'ed */
  lf_allocator_func *destructor;  /* called, when an object is free()'d    */
};

/*
  callback for lf_pinbox_real_free to free a list of unpinned objects -
  add it back to the allocator stack

  DESCRIPTION
    'first' and 'last' are the ends of the linked list of nodes:
    first->el->el->....->el->last. Use first==last to free only one element.
*/
static void alloc_free(void *v_first, void *v_last, void *v_allocator) {
  uchar *first = static_cast<uchar *>(v_first);
  uchar *last = static_cast<uchar *>(v_last);
  LF_ALLOCATOR *allocator = static_cast<LF_ALLOCATOR *>(v_allocator);
  uchar *node = allocator->top;
  do {
    anext_node(last) = node;
  } while (!atomic_compare_exchange_strong(&allocator->top, &node, first) &&
           LF_BACKOFF);
}

/**
  Initialize lock-free allocator.

  @param  allocator           Allocator structure to initialize.
  @param  size                A size of an object to allocate.
  @param  free_ptr_offset     An offset inside the object to a sizeof(void *)
                              memory that is guaranteed to be unused after
                              the object is put in the purgatory. Unused by
                              ANY thread, not only the purgatory owner.
                              This memory will be used to link
                              waiting-to-be-freed objects in a purgatory list.
  @param ctor                 Function to be called after object was
                              malloc()'ed.
  @param dtor                 Function to be called before object is free()'d.
*/

void lf_alloc_init2(LF_ALLOCATOR *allocator, uint size, uint free_ptr_offset,
                    lf_allocator_func *ctor, lf_allocator_func *dtor) {
  lf_pinbox_init(&allocator->pinbox, free_ptr_offset, alloc_free, allocator);
  allocator->top = 0;
  allocator->mallocs = 0;
  allocator->element_size = size;
  allocator->constructor = ctor;
  allocator->destructor = dtor;
  DBUG_ASSERT(size >= sizeof(void *) + free_ptr_offset);
}

/*
  destroy the allocator, free everything that's in it

  NOTE
    As every other init/destroy function here and elsewhere it
    is not thread safe. No, this function is no different, ensure
    that no thread needs the allocator before destroying it.
    We are not responsible for any damage that may be caused by
    accessing the allocator when it is being or has been destroyed.
    Oh yes, and don't put your cat in a microwave.
*/
void lf_alloc_destroy(LF_ALLOCATOR *allocator) {
  uchar *node = allocator->top;
  while (node) {
    uchar *tmp = anext_node(node);
    if (allocator->destructor) {
      allocator->destructor(node);
    }
    lf_free(node);
    node = tmp;
  }
  lf_pinbox_destroy(&allocator->pinbox);
  allocator->top = 0;
}

/*
  Allocate and return an new object.

  DESCRIPTION
    Pop an unused object from the stack or malloc it is the stack is empty.
    pin[0] is used, it's removed on return.
*/
void *lf_alloc_new(LF_PINS *pins) {
  LF_ALLOCATOR *allocator = (LF_ALLOCATOR *)(pins->pinbox->free_func_arg);
  uchar *node;
  for (;;) {
    do {
      node = allocator->top;
      lf_pin(pins, 0, node);
    } while (node != allocator->top && LF_BACKOFF);
    if (!node) {
      node = static_cast<uchar *>(
          lf_alloc(allocator->element_size));
      if (likely(node != 0)) {
        if (allocator->constructor) {
          allocator->constructor(node);
        }
        ++allocator->mallocs;
      }
      break;
    }
    if (atomic_compare_exchange_strong(&allocator->top, &node,
                                       anext_node(node).load())) {
      break;
    }
  }
  lf_unpin(pins, 0);
  return node;
}

/*
  count the number of objects in a pool.

  NOTE
    This is NOT thread-safe !!!
*/
uint lf_alloc_pool_count(LF_ALLOCATOR *allocator) {
  uint i;
  uchar *node;
  for (node = allocator->top, i = 0; node; node = anext_node(node), i++)
    /* no op */;
  return i;
}

static inline void lf_alloc_direct_free(LF_ALLOCATOR *allocator, void *addr) {
  if (allocator->destructor) {
    allocator->destructor((uchar *)addr);
  }
  lf_free(addr);
}

/*
  lock-free lists
*/
#define LF_HASH_UNIQUE 1
typedef bool hash_equal_func(void *, void *, size_t);
typedef bool hash_walk_action(void *);

/* An element of the list */
struct LF_SLIST {
  std::atomic<LF_SLIST *>
      link;      /* a pointer to the next element in a list and a flag */
  uint32 hashnr; /* reversed hash number, for sorting                 */
  const uchar *key;
  size_t keylen;
  /*
    data is stored here, directly after the keylen.
    thus the pointer to data is (void*)(slist_element_ptr+1)
  */
};

const int LF_HASH_OVERHEAD = sizeof(LF_SLIST);

/*
  a structure to pass the context (pointers two the three successive elements
  in a list) from my_lfind to linsert/ldelete
*/
typedef struct {
  std::atomic<LF_SLIST *> *prev;
  LF_SLIST *curr, *next;
} CURSOR;

/*
  the last bit in LF_SLIST::link is a "deleted" flag.
  the helper functions below convert it to a pure pointer or a pure flag
*/
template <class T>
static inline T *PTR(T *ptr) {
  intptr_t i = reinterpret_cast<intptr_t>(ptr);
  i &= (intptr_t)~1;
  return reinterpret_cast<T *>(i);
}

template <class T>
static inline bool DELETED(T *ptr) {
  const intptr_t i = reinterpret_cast<intptr_t>(ptr);
  return i & 1;
}

template <class T>
static inline T *SET_DELETED(T *ptr) {
  intptr_t i = reinterpret_cast<intptr_t>(ptr);
  i |= 1;
  return reinterpret_cast<T *>(i);
}

/*
  DESCRIPTION
    Search for hashnr/key/keylen in the list starting from 'head' and
    position the cursor. The list is ORDER BY hashnr, key
    hash_equal_func: to check whether the cursor_key is our finding key

  RETURN
    0 - not found
    1 - found

  NOTE
    cursor is positioned in either case
    pins[0..2] are used, they are NOT removed on return
*/
static int my_lfind(std::atomic<LF_SLIST *> *head,
                    uint32 hashnr, const uchar *key, size_t keylen,
                    CURSOR *cursor, LF_PINS *pins, hash_equal_func *equal_func, hash_walk_action *walk_action) {
  uint32 cur_hashnr;
  const uchar *cur_key;
  size_t cur_keylen;
  LF_SLIST *link;

retry:
  cursor->prev = head;
  do /* PTR() isn't necessary below, head is a dummy node */
  {
    cursor->curr = (LF_SLIST *)(*cursor->prev);
    lf_pin(pins, 1, cursor->curr);
  } while (*cursor->prev != cursor->curr && LF_BACKOFF);
  for (;;) {
    if (unlikely(!cursor->curr)) {
      return 0; /* end of the list */
    }
    do {
      /* QQ: XXX or goto retry ? */
      link = cursor->curr->link.load();
      cursor->next = PTR(link);
      lf_pin(pins, 0, cursor->next);
    } while (link != cursor->curr->link && LF_BACKOFF);
    cur_hashnr = cursor->curr->hashnr;
    cur_key = cursor->curr->key;
    cur_keylen = cursor->curr->keylen;
    if (*cursor->prev != cursor->curr) {
      (void)LF_BACKOFF;
      goto retry;
    }
    if (!DELETED(link)) {

      if(walk_action && (cur_hashnr & 1)){ 
        // iterate all normal elements
        walk_action(cursor->curr + 1);
      }else if(hashnr & 1 && cur_hashnr & 1 && hashnr == cur_hashnr){ 
        // find a dummy node         
        return 1;
      } else if (!(hashnr & 1 || cur_hashnr & 1) && (*equal_func)((void*)cur_key, (void*)key, cur_keylen)){  
          // find a normal node                      
          return 1;
      } else if (cur_hashnr > hashnr) {
        // out of the bucket
        return 0;
      }
      cursor->prev = &(cursor->curr->link);
      lf_pin(pins, 2, cursor->curr);
    } else {
      /*
        we found a deleted node - be nice, help the other thread
        and remove this deleted node
      */
      if (atomic_compare_exchange_strong(cursor->prev, &cursor->curr,
                                         cursor->next)) {
        lf_pinbox_free(pins, cursor->curr);
      } else {
        (void)LF_BACKOFF;
        goto retry;
      }
    }
    cursor->curr = cursor->next;
    lf_pin(pins, 1, cursor->curr);
  }
}

/*
  DESCRIPTION
    searches for a node as identified by hashnr/keey/keylen in the list
    that starts from 'head'

  RETURN
    0 - not found
    node - found

  NOTE
    it uses pins[0..2], on return the pin[2] keeps the node found
    all other pins are removed.
*/
static LF_SLIST *my_lsearch(std::atomic<LF_SLIST *> *head, uint32 hashnr, const uchar *key, uint keylen,
                            LF_PINS *pins, hash_equal_func *callback) {
  CURSOR cursor;
  int res = my_lfind(head, hashnr, key, keylen, &cursor, pins, callback, 0);

  if (res) {
    lf_pin(pins, 2, cursor.curr);
  } else {
    lf_unpin(pins, 2);
  }
  lf_unpin(pins, 0);
  lf_unpin(pins, 1);
  return res ? cursor.curr : 0;
}

/*
  DESCRIPTION
    insert a 'node' in the list that starts from 'head' in the correct
    position (as found by my_lfind)
    @flag is needed to be hash_unique 
  RETURN
    0     - inserted
    not 0 - a pointer to a duplicate (not pinned and thus unusable)

  NOTE
    it uses pins[0..2], on return all pins are removed.
    if there're nodes with the same key value, a new node is added before them.
*/
static LF_SLIST *linsert(std::atomic<LF_SLIST *> *head,
                         LF_SLIST *node, LF_PINS *pins, uint flags, hash_equal_func *callback) {
  CURSOR cursor;
  int res;

  for (;;) {
    if (my_lfind(head, node->hashnr, node->key, node->keylen, &cursor,
                 pins, callback, 0) &&
        (flags & LF_HASH_UNIQUE)) {
      res = 0; /* duplicate found */
      break;
    } else {
      node->link = cursor.curr;
      DBUG_ASSERT(node->link != node);         /* no circular references */
      DBUG_ASSERT(cursor.prev != &node->link); /* no circular references */
      if (atomic_compare_exchange_strong(cursor.prev, &cursor.curr, node)) {
        res = 1; /* inserted ok */
        break;
      }
    }
  }
  lf_unpin(pins, 0);
  lf_unpin(pins, 1);
  lf_unpin(pins, 2);
  /*
    Note that cursor.curr is not pinned here and the pointer is unreliable,
    the object may dissapear anytime. But if it points to a dummy node, the
    pointer is safe, because dummy nodes are never freed - initialize_bucket()
    uses this fact.
  */
  return res ? 0 : cursor.curr;
}

/*
  DESCRIPTION
    deletes a node as identified by hashnr/keey/keylen from the list
    that starts from 'head'

  RETURN
    0 - ok
    1 - not found

  NOTE
    it uses pins[0..2], on return all pins are removed.
*/
static int ldelete(std::atomic<LF_SLIST *> *head,
                   uint32 hashnr, const uchar *key, uint keylen,
                   LF_PINS *pins, hash_equal_func *callback) {
  CURSOR cursor;
  int res;

  for (;;) {
    if (!my_lfind(head, hashnr, key, keylen, &cursor, pins, callback, 0)) {
      res = 1; /* not found */
      break;
    } else {
      /* mark the node deleted */
      if (atomic_compare_exchange_strong(&cursor.curr->link, &cursor.next,
                                         SET_DELETED(cursor.next))) {
        /* and remove it from the list */
        if (atomic_compare_exchange_strong(cursor.prev, &cursor.curr,
                                           cursor.next)) {
          lf_pinbox_free(pins, cursor.curr);
        } else {
          /*
            somebody already "helped" us and removed the node ?
            Let's check if we need to help that someone too!
            (to ensure the number of "set DELETED flag" actions
            is equal to the number of "remove from the list" actions)
          */
          my_lfind(head, hashnr, key, keylen, &cursor, pins, callback, 0);
        }
        res = 0;
        break;
      }
    }
  }
  lf_unpin(pins, 0);
  lf_unpin(pins, 1);
  lf_unpin(pins, 2);
  return res;
}

/*
  lock-free hash table
*/
#define MAX_LOAD 1 /* average number of elements in a bucket */

struct LF_HASH;
typedef const uchar *(*hash_get_key_function)(const uchar *arg, size_t *length);
typedef ulint lf_hash_func(const uchar *, size_t);
typedef void lf_hash_init_func(uchar *dst, uchar *src);
static const uchar *dummy_key = (uchar *)"";


struct LF_HASH {
  LF_DYNARRAY array;             /* hash itself */
  LF_ALLOCATOR alloc;            /* allocator for elements */
  hash_get_key_function get_key; /* see HASH */
  uint key_offset, key_length;   /* see HASH */
  lf_hash_func *hash_function;   /* see HASH */
  hash_equal_func *equal_func; /* check keys when walking a list of bucket */
  uint element_size;             /* size of memcpy'ed area on insert */
  uint flags;                    /* LF_HASH_UNIQUE, etc */
  std::atomic<int32> size;       /* size of array */
  std::atomic<int32> count;      /* number of elements in the hash */
  int max_load;                  /* average number of elements in a bucket */
  /**
    "Initialize" hook - called to finish initialization of object provided by
     LF_ALLOCATOR (which is pointed by "dst" parameter) and set element key
     from object passed as parameter to lf_hash_insert (pointed by "src"
     parameter). Allows to use LF_HASH with objects which are not "trivially
     copyable".
     NULL value means that element initialization is carried out by copying
     first element_size bytes from object which provided as parameter to
     lf_hash_insert.
  */
  lf_hash_init_func *initialize;
};

static inline const uchar *hash_key(const LF_HASH *hash, const uchar *record,
                                    size_t *length) {
  if (hash->get_key) {
    return (*hash->get_key)(record, length);
  }
  *length = hash->key_length;
  return record + hash->key_offset;
}

/*
  Compute the hash key value from the raw key.

  @note, that the hash value is limited to 2^31, because we need one
  bit to distinguish between normal and dummy nodes.
*/
static inline uint calc_hash(LF_HASH *hash, const uchar *key, size_t keylen) {
  return (hash->hash_function(key, keylen)) & INT_MAX32;
}


/*
  Initializes lf_hash, the arguments are compatible with hash_init

  @note element_size sets both the size of allocated memory block for
  lf_alloc and a size of memcpy'ed block size in lf_hash_insert. Typically
  they are the same, indeed. But LF_HASH::element_size can be decreased
  after lf_hash_init, and then lf_alloc will allocate larger block that
  lf_hash_insert will copy over. It is desireable if part of the element
  is expensive to initialize - for example if there is a mutex or
  DYNAMIC_ARRAY. In this case they should be initialize in the
  LF_ALLOCATOR::constructor, and lf_hash_insert should not overwrite them.
  See wt_init() for example.
  As an alternative to using the above trick with decreasing
  LF_HASH::element_size one can provide an "initialize" hook that will finish
  initialization of object provided by LF_ALLOCATOR and set element key from
  object passed as parameter to lf_hash_insert instead of doing simple memcpy.
*/
void lf_hash_init2(LF_HASH *hash, uint element_size, uint flags,
                   uint key_offset, uint key_length,
                   hash_get_key_function get_key, lf_hash_func *hash_function, 
                   hash_equal_func *equal_func, lf_allocator_func *ctor, 
                   lf_allocator_func *dtor, lf_hash_init_func *init) {
  lf_alloc_init2(&hash->alloc, sizeof(LF_SLIST) + element_size,
                 offsetof(LF_SLIST, key), ctor, dtor);
  lf_dynarray_init(&hash->array, sizeof(LF_SLIST *));
  hash->size = 1;
  hash->count = 0;
  hash->max_load = MAX_LOAD;
  hash->element_size = element_size;
  hash->flags = flags;
  hash->key_offset = key_offset;
  hash->key_length = key_length;
  hash->get_key = get_key;
  hash->hash_function = hash_function;
  hash->equal_func = equal_func;
  hash->initialize = init;
  DBUG_ASSERT(get_key ? !key_offset && !key_length : key_length);
}

void lf_hash_destroy(LF_HASH *hash) {
  LF_SLIST *el, **head = (LF_SLIST **)lf_dynarray_value(&hash->array, 0);

  if (unlikely(!head)) {
    lf_alloc_destroy(&hash->alloc);
    return;
  }
  el = *head;

  while (el) {
    LF_SLIST *next = el->link;
    if (el->hashnr & 1) {
      lf_alloc_direct_free(&hash->alloc, el); /* normal node */
    } else {
      lf_free(el); /* dummy node */
    }
    el = (LF_SLIST *)next;
  }
  lf_alloc_destroy(&hash->alloc);
  lf_dynarray_destroy(&hash->array);
}

/*
  RETURN
    0 - ok
   -1 - out of memory
*/
static int initialize_bucket(LF_HASH *hash, std::atomic<LF_SLIST *> *node,
                             uint bucket, LF_PINS *pins) {
  uint parent = clear_highest_bit(bucket);
  LF_SLIST *dummy =
      (LF_SLIST *)lf_alloc(sizeof(LF_SLIST));
  if (unlikely(!dummy)) {
    return -1;
  }

  LF_SLIST *tmp = 0, *cur;
  std::atomic<LF_SLIST *> *el = static_cast<std::atomic<LF_SLIST *> *>(
      lf_dynarray_lvalue(&hash->array, parent));
  if (unlikely(!el)) {
    lf_free(dummy);
    return -1;
  }
  if (el->load() == nullptr && bucket &&
      unlikely(initialize_bucket(hash, el, parent, pins))) {
    lf_free(dummy);
    return -1;
  }
  dummy->hashnr = reverse_bits(bucket) | 0; /* dummy node */
  dummy->key = dummy_key;
  dummy->keylen = 0;
  if ((cur = linsert(el, dummy, pins, LF_HASH_UNIQUE, hash->equal_func))) {
    lf_free(dummy);
    dummy = cur;
  }
  atomic_compare_exchange_strong(node, &tmp, dummy);
  /*
    note that if the CAS above failed (after linsert() succeeded),
    it would mean that some other thread has executed linsert() for
    the same dummy node, its linsert() failed, it picked up our
    dummy node (in "dummy= cur") and executed the same CAS as above.
    Which means that even if CAS above failed we don't need to retry,
    and we should not free(dummy) - there's no memory leak here
  */
  return 0;
}

/*
  DESCRIPTION
    inserts a new element to a hash. it will have a _copy_ of
    data, not a pointer to it.

  RETURN
    0 - inserted
    1 - didn't (unique key conflict)
   -1 - out of memory

  NOTE
    see linsert() for pin usage notes
*/
int lf_hash_insert(LF_HASH *hash, LF_PINS *pins, void *data) {
  int csize, bucket, hashnr;
  LF_SLIST *node;
  std::atomic<LF_SLIST *> *el;

  node = (LF_SLIST *)lf_alloc_new(pins);
  if (unlikely(!node)) {
    return -1;
  }
  uchar *extra_data =
      (uchar *)(node + 1);  // Stored immediately after the node.
  if (hash->initialize) {
    (*hash->initialize)(extra_data, (uchar*)data);
  } else {
    memcpy(extra_data, data, hash->element_size);
  }
  node->key = hash_key(hash, (uchar *)(node + 1), &node->keylen);
  hashnr = calc_hash(hash, node->key, node->keylen);
  bucket = hashnr % hash->size;
  el = static_cast<std::atomic<LF_SLIST *> *>(
      lf_dynarray_lvalue(&hash->array, bucket));
  if (unlikely(!el)) {
    lf_pinbox_free(pins, node);
    return -1;
  }
  if (el->load() == nullptr &&
      unlikely(initialize_bucket(hash, el, bucket, pins))) {
    lf_pinbox_free(pins, node);
    return -1;
  }
  
  node->hashnr = reverse_bits(hashnr) | 1; /* normal node */
  if (linsert(el, node, pins, hash->flags, hash->equal_func)) {
    lf_pinbox_free(pins, node);
    return 1;
  }
  csize = hash->size;
  if ((hash->count.fetch_add(1) + 1.0) / csize > hash->max_load) {
    atomic_compare_exchange_strong(&hash->size, &csize, csize * 2);
  }
  return 0;
}

/*
  DESCRIPTION
    deletes an element with the given key from the hash (if a hash is
    not unique and there're many elements with this key - the "first"
    matching element is deleted)
  RETURN
    0 - deleted
    1 - didn't (not found)
   -1 - out of memory
  NOTE
    see ldelete() for pin usage notes
*/
int lf_hash_delete(LF_HASH *hash, LF_PINS *pins, const void *key, uint keylen) {
  std::atomic<LF_SLIST *> *el;
  uint bucket, hashnr = calc_hash(hash, (uchar *)key, keylen);

  bucket = hashnr % hash->size;
  el = static_cast<std::atomic<LF_SLIST *> *>(
      lf_dynarray_lvalue(&hash->array, bucket));
  if (unlikely(!el)) {
    return -1;
  }
  /*
    note that we still need to initialize_bucket here,
    we cannot return "node not found", because an old bucket of that
    node may've been split and the node was assigned to a new bucket
    that was never accessed before and thus is not initialized.
  */
  if (el->load() == nullptr &&
      unlikely(initialize_bucket(hash, el, bucket, pins))) {
    return -1;
  }
  if (ldelete(el, reverse_bits(hashnr) | 1, (uchar *)key,
              keylen, pins, hash->equal_func)) {
    return 1;
  }
  --hash->count;
  return 0;
}

/**
  Find hash element corresponding to the key.

  @param hash    The hash to search element in.
  @param pins    Pins for the calling thread which were earlier
                 obtained from this hash using lf_hash_get_pins().
  @param key     Key
  @param keylen  Key length

  @retval A pointer to an element with the given key (if a hash is not unique
          and there're many elements with this key - the "first" matching
          element).
  @retval NULL         - if nothing is found

  @note Uses pins[0..2]. On return pins[0..1] are removed and pins[2]
        is used to pin object found. It is also not removed in case when
        object is not found/error occurs but pin value is undefined in
        this case.
        So calling lf_hash_unpin() is mandatory after call to this function
        in case of both success and failure.
        @sa my_lsearch().
*/

void *lf_hash_search(LF_HASH *hash, LF_PINS *pins, const void *key,
                     uint keylen) {
  std::atomic<LF_SLIST *> *el;
  LF_SLIST *found;
  uint bucket, hashnr = calc_hash(hash, (uchar *)key, keylen);

  bucket = hashnr % hash->size;

  el = static_cast<std::atomic<LF_SLIST *> *>(
      lf_dynarray_lvalue(&hash->array, bucket));
  if (unlikely(!el)) {
    return 0;
  }
  if (el->load() == nullptr &&
      unlikely(initialize_bucket(hash, el, bucket, pins))) {
    return 0;
  }

  found = my_lsearch(el, reverse_bits(hashnr) | 1, (uchar *)key, keylen, pins, hash->equal_func);
  return found ? found + 1 : 0;
}

/**
  Iterate over all elements in hash and call function with the element

  @note
  If one of 'action' invocations returns 1 the iteration aborts.
  'action' might see some elements twice!

  @retval 0    ok
  @retval 1    error (action returned 1)
  */
int lf_hash_iterate(LF_HASH *hash, LF_PINS *pins, hash_walk_action action)
{
  CURSOR cursor;
  uint bucket= 0;
  int res;
  std::atomic<LF_SLIST *> *el;

  el = static_cast<std::atomic<LF_SLIST *> *>(
      lf_dynarray_lvalue(&hash->array, bucket));

  if (unlikely(!el))
    return 0; /* if there's no bucket==0, the hash is empty */
  if (el->load() == NULL && unlikely(initialize_bucket(hash, el, bucket, pins)))
    return 0; /* if there's no bucket==0, the hash is empty */

  res= my_lfind(el, 0, 0, 0, &cursor, pins, hash->equal_func, action);

  lf_unpin(pins, 2);
  lf_unpin(pins, 1);
  lf_unpin(pins, 0);

  return res;
}

/*
  only for test
*/
/*
  test lf_dynarray
*/
void test_lf_dynarray(){
  using namespace std;
    LF_DYNARRAY arr;
    lf_dynarray_init(&arr, sizeof(int));

    int * ptr = static_cast<int *>(lf_dynarray_lvalue(&arr, 3)); 
    *ptr = 0;
    ++ *ptr;
    int * ptr2 = static_cast<int *>(lf_dynarray_lvalue(&arr, 3)); 

    lf_dynarray_destroy(&arr);
}

/*
  test lf_hash
*/

struct key_value{
  ulint key;
  ulint val;
};

// hash function for key
ulint kv_hash_function(const uchar *key,        /*!< in: value to be hashed */
                    size_t key_len) /*!< in: hash table size */
{
  ulint *tmp = (ulint *)key;

  return (*tmp) ^ 1653893711;
}

// func for get pointer of key from record
static const uchar *kv_hash_get_key(const uchar *record, size_t *key_len) {
  key_value * kv = (key_value *)record;
  *key_len = sizeof(kv->key); // note that key_len should be assigned
  ulint *p = (ulint *)(record + offsetof(key_value, key));
  return record + offsetof(key_value, key);
}

// func for check whether key1 == key2
bool kv_hash_equal_func(void *key1, void *key2, size_t key_len){
  ulint *tmp1 = (ulint *)key1;
  ulint *tmp2 = (ulint *)key2;
  return tmp1 == tmp2;
}

int cnt = 0;
bool kv_walk_action(void * record){
  cnt++;
  // key_value * kv = (key_value *)record;
  // std::cout << kv->key << "," << kv->val << std::endl;
  return 0;
}

/* test for single thread */
void test_lf_hash(){
  using namespace std;

  /* init a LF_HASH*/
  LF_HASH m_hash;
  lf_hash_init2(&m_hash, sizeof(key_value), LF_HASH_UNIQUE, 0, 0, kv_hash_get_key, &kv_hash_function, &kv_hash_equal_func, NULL, NULL, NULL);

  // get a LF_PINS of m_hash for a thread
  LF_PINS *pins = lf_pinbox_get_pins(&m_hash.alloc.pinbox);

  // insert an key_value
  key_value kv1 = {1, 4};
  lf_hash_insert(&m_hash, pins, &kv1);
  // insert an key_value
  key_value kv11 = {3, 4};
  lf_hash_insert(&m_hash, pins, &kv11);
    // insert an key_value
  key_value kv12 = {2, 4};
  lf_hash_insert(&m_hash, pins, &kv12);

  // search an key_value
  ulint key1 = 2;
  key_value *kv2 = (key_value *)lf_hash_search(&m_hash, pins, &key1, sizeof(key1));
  if(kv2!=nullptr){
    cout << kv2->key << "," << kv2->val << endl;
    lf_unpin(pins, 2); // if we found, we need to unpin 2
  }
  

  // delete an key_value
  ulint key2 = 2;
  lf_hash_delete(&m_hash, pins, &key1, sizeof(key1));
  key_value *kv3 = (key_value *)lf_hash_search(&m_hash, pins, &key2, sizeof(key2));
  if(kv3!=nullptr){
    cout << kv3->key << "," << kv3->val << endl;
    lf_unpin(pins, 2); // if we found, we need to unpin 2
  }else{
    cout << "kv3 is nullptr" << endl;
  }
  
  // iterate all elements
  lf_hash_iterate(&m_hash, pins, &kv_walk_action);

  // return pins to pinbox
  lf_pinbox_put_pins(pins);

}

/* test for multi thread */

#include <unistd.h>
LF_HASH m_hash;

int thread_num;

void *func(void *arg){
  using namespace std;

  // get a LF_PINS of m_hash for a thread
  LF_PINS *pins = lf_pinbox_get_pins(&m_hash.alloc.pinbox);

  int id = *(int *)&arg;
  key_value kv1 = {4, 4};
  for (int i = 0; i < 1000000; i++) {
    kv1 = {i + id, i + id};
    lf_hash_insert(&m_hash, pins, &kv1);
  }
  // // insert an key_value
  // key_value kv1 = {4, 4};
  // // insert an key_value
  // key_value kv11 = {2, 4};
  // lf_hash_insert(&m_hash, pins, &kv11);
  //   // insert an key_value
  // key_value kv12 = {3, 4};
  // lf_hash_insert(&m_hash, pins, &kv12);

  // // search an key_value
  // ulint key1 = 2;
  // key_value *kv2 = (key_value *)lf_hash_search(&m_hash, pins, &key1, sizeof(key1));
  // if(kv2!=nullptr){
  //   cout << "t2: " <<  kv2->key << "," << kv2->val << endl;
  //   lf_unpin(pins, 2); // if we found, we need to unpin 2
  // }
  

  // // delete an key_value
  // ulint key2 = 2;
  // lf_hash_delete(&m_hash, pins, &key1, sizeof(key1));
  // key_value *kv3 = (key_value *)lf_hash_search(&m_hash, pins, &key2, sizeof(key2));
  // if(kv3!=nullptr){
  //   cout << "t2: " << kv3->key << "," << kv3->val << endl;
  //   lf_unpin(pins, 2); // if we found, we need to unpin 2
  // }else{
  //   cout << "t2: " << "kv3 is nullptr" << endl;
  // }

  // return pins to pinbox
  lf_pinbox_put_pins(pins);

}


void test_lf_hash_mutilthreads(){

  using namespace std;
  /* init a LF_HASH*/
  lf_hash_init2(&m_hash, sizeof(key_value), LF_HASH_UNIQUE, 0, 0, kv_hash_get_key, &kv_hash_function, &kv_hash_equal_func, NULL, NULL, NULL);

  
  thread_num = 16;
  pthread_t tid[thread_num];

  for (int i = 0; i < thread_num; i++) {
    pthread_create(&tid[i], NULL, func, (void *)i);
  }
  for (int i = 0; i < thread_num; i++) {
    pthread_join(tid[i], NULL);
  }

  // get a LF_PINS of m_hash for a thread
  LF_PINS *pins = lf_pinbox_get_pins(&m_hash.alloc.pinbox);

  lf_hash_iterate(&m_hash, pins, &kv_walk_action);

  // return pins to pinbox
  lf_pinbox_put_pins(pins);

}


int main(){
  // test_lf_hash();
  test_lf_hash_mutilthreads();
    
  printf("hehe\n");

  return 0;
}


#endif
