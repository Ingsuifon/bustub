//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) { 
  clock.resize(num_pages, std::pair<bool, bool>(false, false));
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  while (size) {
    if (clock[next].first) {
      if (!clock[next].second) {
        Pin(next);
        *frame_id = next++;
        next %= clock.size();
        return true;
      } else {
        clock[next].second = false;
      }
    }
    next++;
    next %= clock.size();
  }
  return false; 
}

void ClockReplacer::Pin(frame_id_t frame_id) { 
  if (clock[frame_id].first) {
    clock[frame_id].first = false;
    size--;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (!clock[frame_id].first) {
    clock[frame_id].first = true;
    clock[frame_id].second = true;
    size++;
  }
}

size_t ClockReplacer::Size() { return size; }

}  // namespace bustub
