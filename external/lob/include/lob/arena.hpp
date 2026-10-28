#pragma once
// Big, zero-initialised, preallocated memory regions.
//
// We reserve all memory once at startup with mmap. Linux hands out zero pages lazily, so a
// large reservation only costs physical RAM for the pages actually touched. Call prefault()
// to touch every page up front if you want zero page faults on the hot path.

#include <sys/mman.h>

#include <cstddef>
#include <cstring>
#include <new>
#include <stdexcept>
#include <utility>

namespace lob {

template <class T>
class Arena {
 public:
  Arena() = default;
  explicit Arena(std::size_t count, bool huge_pages = true) : count_(count) {
    if (count == 0) return;
    bytes_ = count * sizeof(T);
    void* p = ::mmap(nullptr, bytes_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (p == MAP_FAILED) throw std::bad_alloc();
#ifdef MADV_HUGEPAGE
    // Transparent huge pages: 2 MiB pages mean far fewer TLB misses for random access.
    if (huge_pages) ::madvise(p, bytes_, MADV_HUGEPAGE);
#endif
    data_ = static_cast<T*>(p);
  }
  ~Arena() {
    if (data_) ::munmap(data_, bytes_);
  }
  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;
  Arena(Arena&& o) noexcept { swap(o); }
  Arena& operator=(Arena&& o) noexcept {
    Arena tmp(std::move(o));
    swap(tmp);
    return *this;
  }
  void swap(Arena& o) noexcept {
    std::swap(data_, o.data_);
    std::swap(count_, o.count_);
    std::swap(bytes_, o.bytes_);
  }

  // Touch every page so later accesses never page-fault.
  void prefault() {
    volatile char* c = reinterpret_cast<volatile char*>(data_);
    for (std::size_t i = 0; i < bytes_; i += 4096) c[i] = 0;
  }

  T* data() const { return data_; }
  std::size_t size() const { return count_; }
  std::size_t bytes() const { return bytes_; }
  T& operator[](std::size_t i) const { return data_[i]; }

 private:
  T* data_ = nullptr;
  std::size_t count_ = 0;
  std::size_t bytes_ = 0;
};

}  // namespace lob
