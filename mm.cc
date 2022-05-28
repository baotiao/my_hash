#include "ut0cpu_cache.h"

#include <atomic>

constexpr size_t INNODB_CACHE_LINE_SIZE = 64;

/** Multiple producer consumer, bounded queue
 Implementation of Dmitry Vyukov's MPMC algorithm
 http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue */
template <typename T>
class mpmc_bq {
 public:
  /** Constructor
  @param[in]	n_elems		Max number of elements allowed */
  explicit mpmc_bq(size_t n_elems)
      : m_ring(reinterpret_cast<Cell *>(UT_NEW_ARRAY_NOKEY(Aligned, n_elems))),
        m_capacity(n_elems - 1) {
    /* Should be a power of 2 */
    ut_a((n_elems >= 2) && ((n_elems & (n_elems - 1)) == 0));

    for (size_t i = 0; i < n_elems; ++i) {
      m_ring[i].m_pos.store(i, std::memory_order_relaxed);
    }

    m_enqueue_pos.store(0, std::memory_order_relaxed);
    m_dequeue_pos.store(0, std::memory_order_relaxed);
  }

  /** Destructor */
  ~mpmc_bq() { UT_DELETE_ARRAY(m_ring); }

  /** Enqueue an element
  @param[in]	data		Element to insert, it will be copied
  @return true on success */
  bool enqueue(T const &data) MY_ATTRIBUTE((warn_unused_result)) {
    /* m_enqueue_pos only wraps at MAX(m_enqueue_pos), instead
    we use the capacity to convert the sequence to an array
    index. This is why the ring buffer must be a size which
    is a power of 2. This also allows the sequence to double
    as a ticket/lock. */

    // pos 是enqueue 时刻, 想要插入的位置
    size_t pos = m_enqueue_pos.load(std::memory_order_relaxed);

    Cell *cell;

    for (;;) {
      // 比如初始化的时候m_enqueue_pos = 0
      // cell 获得m_ring 循环数组 0 这个位置
      cell = &m_ring[pos & m_capacity];

      size_t seq;

      // seq = 0, 当前cell 在m_ring 的位置
      seq = cell->m_pos.load(std::memory_order_acquire);

      // seq 是当前cell 的位置和pos 是否有变化
      // 如果在获得pos 和 cell 之前已经有其他thread 已经插入数据了
      // 那么这个时候sel = 1, 那么diff = 1
      // 所以diff == 0 说明要插入的位置就是enqueue 时候要插入的位置
      // 如果diff !=0 说明要插入的已经被别人插入了, 顺势去插入下个位置了
      // 如果diff < 0 说明队列已经满了
      intptr_t diff = (intptr_t)seq - (intptr_t)pos;

      /* If they are the same then it means this cell is empty */

      if (diff == 0) {
        /* Claim our spot by moving head. If head isn't the same as we last
        checked then that means someone beat us to the punch. Weak compare is
        faster, but can return spurious results which in this instance is OK,
        because it's in the loop */

        // diff == 0, 说明这次插入可以成功插入
        // 插入完成以后, 需要把调整下次要插入的位置, 也就是m_enqueue_pos + 1
        if (m_enqueue_pos.compare_exchange_weak(pos, pos + 1,
                                                std::memory_order_relaxed)) {
          break;
        }

      } else if (diff < 0) {
        /* The queue is full */

        return (false);

      } else {
        pos = m_enqueue_pos.load(std::memory_order_relaxed);
      }
    }

    cell->m_data = data;

    /* Increment the sequence so that the tail knows it's accessible */

    cell->m_pos.store(pos + 1, std::memory_order_release);

    return (true);
  }

  /** Dequeue an element
  @param[out]	data		Element read from the queue
  @return true on success */
  bool dequeue(T &data) MY_ATTRIBUTE((warn_unused_result)) {
    Cell *cell;
    size_t pos = m_dequeue_pos.load(std::memory_order_relaxed);

    // 这里dequeue 会保证按照顺序进行dequeue
    for (;;) {
      cell = &m_ring[pos & m_capacity];

      size_t seq = cell->m_pos.load(std::memory_order_acquire);

      auto diff = (intptr_t)seq - (intptr_t)(pos + 1);

      if (diff == 0) {
        /* Claim our spot by moving the head. If head isn't the same as we last
        checked then that means someone beat us to the punch. Weak compare is
        faster, but can return spurious results. Which in this instance is
        OK, because it's in the loop. */

        if (m_dequeue_pos.compare_exchange_weak(pos, pos + 1,
                                                std::memory_order_relaxed)) {
          break;
        }

      } else if (diff < 0) {
        /* The queue is empty */
        return (false);

      } else {
        /* Under normal circumstances this branch should never be taken. */
        pos = m_dequeue_pos.load(std::memory_order_relaxed);
      }
    }

    data = cell->m_data;

    /* Set the sequence to what the head sequence should be next
    time around */

    cell->m_pos.store(pos + m_capacity + 1, std::memory_order_release);

    return (true);
  }

  /** @return the capacity of the queue */
  size_t capacity() const MY_ATTRIBUTE((warn_unused_result)) {
    return (m_capacity + 1);
  }

  /** @return true if the queue is empty. */
  bool empty() const MY_ATTRIBUTE((warn_unused_result)) {
    size_t pos = m_dequeue_pos.load(std::memory_order_relaxed);

    for (;;) {
      auto cell = &m_ring[pos & m_capacity];

      size_t seq = cell->m_pos.load(std::memory_order_acquire);

      auto diff = (intptr_t)seq - (intptr_t)(pos + 1);

      if (diff == 0) {
        return (false);
      } else if (diff < 0) {
        return (true);
      } else {
        pos = m_dequeue_pos.load(std::memory_order_relaxed);
      }
    }

    return (false);
  }

 private:
  using Pad = byte[ut::INNODB_CACHE_LINE_SIZE];

  struct Cell {
    std::atomic<size_t> m_pos;
    T m_data;
  };

  using Aligned =
      typename std::aligned_storage<sizeof(Cell),
                                    std::alignment_of<Cell>::value>::type;

  Pad m_pad0;
  Cell *const m_ring;
  size_t const m_capacity;
  Pad m_pad1;
  std::atomic<size_t> m_enqueue_pos;
  Pad m_pad2;
  std::atomic<size_t> m_dequeue_pos;
  Pad m_pad3;

  mpmc_bq(mpmc_bq &&) = delete;
  mpmc_bq(const mpmc_bq &) = delete;
  mpmc_bq &operator=(mpmc_bq &&) = delete;
  mpmc_bq &operator=(const mpmc_bq &) = delete;
};

#endif /* ut0mpmcbq_h */
