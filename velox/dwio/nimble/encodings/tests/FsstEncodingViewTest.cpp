/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/dwio/nimble/encodings/FsstEncoding.h"

#include <limits>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "velox/dwio/nimble/encodings/tests/EncodingViewTestUtils.h"

namespace facebook::nimble::test {
namespace {

class FsstEncodingViewTest : public EncodingViewTest {
 protected:
  Encoding::Options forceFsstOptions() const {
    return {.fsstCompressionTargetRatio = std::numeric_limits<double>::max()};
  }

  Vector<std::string_view> makeValues(
      std::vector<std::string>& storage,
      uint32_t rowCount) {
    storage.reserve(rowCount);
    for (uint32_t row = 0; row < rowCount; ++row) {
      switch (row % 5) {
        case 0:
          storage.emplace_back(
              "https://www.example.com/experiments/targeting/" +
              std::to_string(row % 37));
          break;
        case 1:
          storage.emplace_back(
              "qrt_exposure_info_v2:campaign:placement:country:" +
              std::to_string(row % 19));
          break;
        case 2:
          storage.emplace_back();
          break;
        case 3:
          storage.emplace_back("binary\0payload\0", 15);
          break;
        default:
          storage.emplace_back(
              "repeated production-shaped log message for FSST decoding");
          break;
      }
    }

    Vector<std::string_view> values{pool_.get()};
    values.reserve(storage.size());
    for (const auto& value : storage) {
      values.emplace_back(value);
    }
    return values;
  }
};

TEST_F(FsstEncodingViewTest, readsAcrossLazyChunkBoundaries) {
  std::vector<std::string> storage;
  auto values = makeValues(storage, 2051);
  const std::vector<uint32_t> positions{
      0, 1, 2, 1022, 1023, 1024, 1025, 2047, 2048, 2050};

  expectReads<FsstEncoding>(values, positions, forceFsstOptions());
  expectReads<FsstEncoding>(
      values, positions, forceFsstOptions(), CompressionType::Lz4);
}

TEST_F(FsstEncodingViewTest, supportsConcurrentReadsAcrossChunks) {
  std::vector<std::string> storage;
  auto values = makeValues(storage, 2048);
  std::mt19937 rng{42};
  std::uniform_int_distribution<uint32_t> row{
      0, static_cast<uint32_t>(values.size() - 1)};
  std::vector<uint32_t> positions;
  positions.reserve(4096);
  for (uint32_t i = 0; i < 4096; ++i) {
    positions.push_back(row(rng));
  }

  expectConcurrentReads<FsstEncoding>(values, positions, forceFsstOptions());
}

} // namespace
} // namespace facebook::nimble::test
