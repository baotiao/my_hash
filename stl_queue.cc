#include <atomic>
#include <queue>
#include <cstddef>
#include <iostream>
#include <assert.h>
#include <stddef.h>
#include <atomic>
#include <algorithm>
#include <sys/types.h>
#include <string.h>
#include <thread>
#include <unistd.h>
#include <sys/time.h>

using namespace std;

queue <int> q;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

int thread_num = 0;
int element_num = 1000000;

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void *func(void *arg) {
  int id = *(int *)&arg;
  for (int i = 0; i < element_num; i++) {
    // printf("i %d\n", i);
    pthread_mutex_lock(&lock);
    q.push(i);
    pthread_mutex_unlock(&lock);
    // while (!q.push(i)) {
    //   std::this_thread::yield();
    // }
  }
}

void *func1(void *arg) {
  int id = *(int *)&arg;
  int data;
  for (int i = 0; i < element_num; i++) {
    pthread_mutex_lock(&lock);
    while (!q.empty()) {
      q.pop();
    }
    pthread_mutex_unlock(&lock);
  }
}

void test_mcmq_mutilthreads() {
  thread_num = 8;
  pthread_t tid[thread_num];

  pthread_t consumer[thread_num];
  uint64_t st, ed;
  st = NowMicros();
  for (int i = 0; i < thread_num; i++) {
    pthread_create(&tid[i], NULL, func, (void *)i);
  }
  for (int i = 0; i < thread_num; i++) {
    pthread_create(&consumer[i], NULL, func1, (void *)i);
  }
  for (int i = 0; i < thread_num; i++) {
    pthread_join(tid[i], NULL);
  }
  for (int i = 0; i < thread_num; i++) {
    pthread_join(consumer[i], NULL);
  }

  while (!q.empty()) {
    q.pop();
  }
  ed = NowMicros();

  printf("insert %lld elements, time cost %lld us\n", (uint64_t)thread_num * (uint64_t)element_num, ed - st);

}


int main()
{
  test_mcmq_mutilthreads();
  return 0;
}
