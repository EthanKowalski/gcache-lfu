#include <cassert>
#include <cstdint>
#include <iostream>
#include <ostream>
#include <stdexcept>

#include "gcache/lfu_cache.h"
#include "util.h"

using namespace gcache;

struct hash1 {
  uint32_t operator()(uint32_t x) const noexcept { return x + 1000; }
};

struct hash2 {
  uint32_t operator()(uint32_t x) const noexcept { return x; }
};

void test() {
  LFUCache<uint32_t, uint32_t, hash1> cache;
  cache.init(4);
  assert(cache.size() == 0);
  assert(cache.capacity() == 4);

  auto h1 = cache.insert(1, true); // f1 = 1
  assert(h1);
  assert(cache.size() == 1);
  *h1 = 111;
  auto h2 = cache.insert(2, true); // f2 = 1
  assert(h2);
  assert(cache.size() == 2);
  *h1 = 222;
  auto h3 = cache.insert(3, true); // f3 = 1
  assert(h3);
  assert(cache.size() == 3);
  *h1 = 333;
  auto h4 = cache.insert(4); // f4 = 1
  assert(h4);
  assert(cache.size() == 4);
  *h1 = 444;
  std::cout << "=== Expect: lfu: [4], in_use: [1, 2, 3] ===\n";
  std::cout << cache;

  h4 = cache.lookup(4, true); // f4 = 2
  *h4 = 4444;
  assert(cache.size() == 4);
  std::cout << "=== Expect: lfu: [], in_use: [1, 2, 3, 4] ===\n";
  std::cout << cache;

  auto h5 = cache.insert(5, true); // does not insert
  if (h5 != nullptr)
    throw std::runtime_error("Overflow insertion is not denied!");
  assert(cache.size() == 4);

  cache.release(h3); // f3 = 2
  h5 = cache.insert(5, true); // f5 = 1
  assert(h5);
  assert(cache.size() == 4);
  *h5 = 555;
  std::cout << "\n=== Expect: in_use: [1, 2, 4, 5] ===\n";
  std::cout << cache;

  cache.release(h5); // f5 = 2
  cache.release(h2); // f2 = 2
  cache.release(h4); // f4 = 3
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [5, 2, 4], in_use: [1] ===\n";
  std::cout << cache;

  h3 = cache.insert(3, true); // f3 = 1
  assert(h3);
  assert(cache.size() == 4);
  *h3 = 3333;
  h5 = cache.lookup(5, true); // does not exist
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [2, 4], in_use: [1, 3] ===\n";
  std::cout << cache;
  if (h5 != nullptr)
    throw std::runtime_error("Expected evicted handle remains in cache!");

  h5 = cache.insert(5, true); // f5 = 1
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [4], in_use: [1, 3, 5] ===\n";
  std::cout << cache;

  auto h6 = cache.insert(6, true); // f6 = 1
  assert(h6);
  assert(cache.size() == 4);
  *h6 = 666;
  std::cout << "\n=== Expect: lfu: [], in_use: [1, 3, 5, 6] ===\n";
  std::cout << cache;

  auto h5_ = cache.insert(5, true); // f5 = 2 (using different handle)
  assert(h5_ == h5);
  assert(cache.size() == 4);
  *h5_ = 555;
  std::cout << "\n=== Expect: lfu: [], in_use: [1, 3, 5, 6] ===\n";
  std::cout << cache;

  auto h7 = cache.insert(7, true); // does not insert
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [], in_use: [1, 3, 5, 6] ===\n";
  std::cout << cache;
  if (h7) throw std::runtime_error("Overflow handle is not denied!");

  cache.release(h1); // f1 = 2
  cache.release(h3); // f3 = 2
  cache.release(h5); // f5 = 3, handle h5_ is still pinned
  cache.release(h6); // f6 = 2
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [1, 3, 6], in_use: [5] ===\n";
  std::cout << cache;

  h3 = cache.lookup(3); // f3 = 3
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [1, 6, 3], in_use: [5] ===\n";
  std::cout << cache;

  cache.release(h5_); // f5 = 4
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [1, 6, 3, 5], in_use: [] ===\n";
  std::cout << cache;

  h5 = cache.lookup(5); // f5 = 5
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [1, 6, 3, 5], in_use: [] ===\n";
  std::cout << cache;

  h6 = cache.lookup(6, true); // f6 = 3
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [1, 3, 5], in_use: [6] ===\n";
  std::cout << cache;

  cache.release(h6); // f6 = 4
  assert(cache.size() == 4);
  std::cout << "\n=== Expect: lfu: [1, 3, 6, 5], in_use: [] ===\n";
  std::cout << cache;

  h7 = cache.lookup(7);
  if (h7) throw std::runtime_error("Lookup nonexisting handle is not denied!");

  h7 = cache.insert(7); // f7 = 1
  assert(h7);
  assert(cache.size() == 4);
  *h7 = 777;
  std::cout << "\n=== Expect: lfu: [7, 3, 6, 5], in_use: [] ===\n";
  std::cout << cache;

  // test erase/install
  bool success = cache.erase(h7);
  assert(success);
  assert(cache.size() == 3);
  assert(cache.capacity() == 3);
  std::cout << "\n=== Expect: lfu: [3, 6, 5], in_use: [] ===\n";
  std::cout << cache;

  h6 = cache.lookup(6, true); // f6 = 5
  assert(h6);
  assert(cache.size() == 3);
  std::cout << "\n=== Expect: lfu: [3, 5], in_use: [6] ===\n";
  std::cout << cache;
  success = cache.erase(h6);
  if (success) throw std::runtime_error("Erase in-use handle is not denied!");

  auto h8 = cache.insert(8); // f8 = 1
  *h8 = 888;
  assert(cache.size() == 3);
  std::cout << "\n=== Expect: lfu: [8, 5], in_use: [6] ===\n";
  std::cout << cache;

  auto h9 = cache.install(9); // f9 = 1
  assert(h9);
  assert(cache.size() == 4);
  assert(cache.capacity() == 4);
  *h9 = 999;
  std::cout << "\n=== Expect: lfu: [8, 9, 5], in_use: [6] ===\n";
  std::cout << cache;

  // test for_each
  std::cout << "\n=== Expect: { 5: 555, 8: 888, 9: 999, 6: 666, } ===\n";
  std::cout << "{ ";
  cache.for_each([](LFUCache<uint32_t, uint32_t, hash1>::Handle_t h) {
    std::cout << h.get_key() << ": " << *h << ", ";
  });
  std::cout << "}" << std::endl;

  cache.release(h6); // f6 = 6
  std::cout << "\n=== Expect: lfu: [8, 9, 5, 6], in_use: [] ===\n";
  std::cout << cache;

  // test for checkpoint and recover (would keep order, but lose frequency?)
  std::vector<std::pair<uint32_t, uint32_t>> ckpt;
  cache.for_each_lfu([&ckpt](LFUCache<uint32_t, uint32_t, hash1>::Handle_t h) {
    ckpt.emplace_back(h.get_key(), *h);
  });

  LFUCache<uint32_t, uint32_t, hash1> cache_recovered;
  cache_recovered.init(4);
  for (auto& [key, value] : ckpt) {
    auto h = cache_recovered.insert(key, true); // all f = 1 on recovery
    assert(h);
    *h = value;
    cache_recovered.release(h);
  }
  std::cout << "\n=== Expect: { 5: 555, 6: 666, 8: 888, 9: 999, } ===\n";
  std::cout << cache_recovered;

  std::cout << std::flush;
}

void bench() {

  int numElements = 16 * 1024;

  LFUCache<uint32_t, uint32_t, hash2> cache;
  cache.init(numElements);  // #blocks for 1GB working set

  // filling the cache
  auto ts0 = rdtsc();
  for (int i = 0; i < numElements; ++i) cache.insert(i);
  auto ts1 = rdtsc();

  // cache hit
  for (int i = 0; i < numElements; ++i) cache.insert(i);
  auto ts2 = rdtsc();

  // cache miss
  for (int i = 0; i < numElements; ++i) cache.insert(i + numElements);
  auto ts3 = rdtsc();

  std::cout << "Fill: " << (ts1 - ts0) / (numElements) << " cycles/op\n";
  std::cout << "Hit:  " << (ts2 - ts1) / (numElements) << " cycles/op\n";
  std::cout << "Miss: " << (ts3 - ts2) / (numElements) << " cycles/op\n";
  std::cout << std::flush;
}

int main() {
  test();   // for correctness
  bench();  // for performance
  return 0;
}
