#pragma once
// Small shared utilities: thread pinning, clocks, command-line options.

#include <pthread.h>
#include <sched.h>
#include <time.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace feed {

// Pin the calling thread to one CPU. A pinned thread keeps its caches and branch predictors warm
// and is never migrated mid-burst by the scheduler. Returns false (and warns) if not allowed.
inline bool pin_this_thread(int cpu, const char* name) {
  if (cpu < 0) return true;
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof set, &set);
  if (rc != 0) {
    std::fprintf(stderr, "warning: could not pin %s to cpu %d: %s\n", name, cpu, std::strerror(rc));
    return false;
  }
  pthread_setname_np(pthread_self(), name);
  return true;
}

inline std::int64_t realtime_ns() {
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return std::int64_t(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

inline std::int64_t monotonic_ns() {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return std::int64_t(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

// Minimal "--key value" / "--flag" parser. Unknown keys are an error so typos do not silently
// fall back to defaults.
class Options {
 public:
  Options(int argc, char** argv, std::map<std::string, std::string> defaults, std::string usage)
      : vals_(std::move(defaults)), usage_(std::move(usage)) {
    for (int i = 1; i < argc; ++i) {
      std::string a = argv[i];
      if (a == "-h" || a == "--help") {
        std::printf("%s", usage_.c_str());
        std::exit(0);
      }
      if (a.rfind("--", 0) != 0) {
        positional_.push_back(a);
        continue;
      }
      std::string k = a.substr(2);
      auto it = vals_.find(k);
      if (it == vals_.end()) fail("unknown option --" + k);
      if (it->second == "false" || it->second == "true") {  // boolean flag
        it->second = "true";
      } else {
        if (i + 1 >= argc) fail("missing value for --" + k);
        it->second = argv[++i];
      }
    }
  }

  std::string str(const std::string& k) const { return vals_.at(k); }
  long long num(const std::string& k) const { return std::stoll(vals_.at(k)); }
  double real(const std::string& k) const { return std::stod(vals_.at(k)); }
  bool flag(const std::string& k) const { return vals_.at(k) == "true"; }
  const std::vector<std::string>& positional() const { return positional_; }

  [[noreturn]] void fail(const std::string& msg) const {
    std::fprintf(stderr, "error: %s\n\n%s", msg.c_str(), usage_.c_str());
    std::exit(2);
  }

 private:
  std::map<std::string, std::string> vals_;
  std::vector<std::string> positional_;
  std::string usage_;
};

}  // namespace feed
