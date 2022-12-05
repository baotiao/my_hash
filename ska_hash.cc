#include <iostream>
#include <map>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdint.h>
#include <random>
#include <assert.h>
#include <string.h>

#include "flat_hash_map.hpp"

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

ska::flat_hash_map<int, int> m_hash;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

int thread_num = 1;
int element_num = 10000000;


void *func(void *arg) {
  using namespace std;

  int id = *(int *)&arg;
  for (int i = 0; i < element_num; i++) {
    // pthread_mutex_lock(&lock);
    m_hash[i + id] = i + id;
    // pthread_mutex_unlock(&lock);
  }
}

void test_hash() {
  m_hash.clear();
  thread_num = 1;
  pthread_t tid[thread_num];

  uint64_t st, ed;

  
  st = NowMicros();
  for (int i = 0; i < thread_num; i++) {
    pthread_create(&tid[i], NULL, func, (void *)i);
  }
  for (int i = 0; i < thread_num; i++) {
    pthread_join(tid[i], NULL);
  }
  ed = NowMicros();

  printf("insert %lld elements, time cost %lld us\n", (uint64_t)thread_num * (uint64_t)element_num, ed - st);
}
int main()
{
  test_hash();


  return 0;
}
