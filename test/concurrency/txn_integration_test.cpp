#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>  // NOLINT
#include <cstdio>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>  //NOLINT
#include <utility>
#include <vector>

#include "common_checker.h"  // NOLINT

namespace bustub {

void CheckTableLock(bustub::Transaction *txn) {
  if (!txn->GetExclusiveTableLockSet()->empty() || !txn->GetSharedTableLockSet()->empty()) {
    fmt::print("should not acquire S/X table lock, grab IS/IX instead");
    exit(1);
  }
}

void CommitTest1() {
  // should scan changes of committed txn
  auto db = GetDbForCommitAbortTest("CommitTest1");
  auto txn1 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Insert(txn1, *db, 1);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Scan(txn2, *db, {1, 233, 234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(CommitAbortTest, DISABLED_CommitTestA) { CommitTest1(); }

void Test1(IsolationLevel lvl) {
  // should scan changes of committed txn
  auto db = GetDbForVisibilityTest("Test1");
  auto txn1 = Begin(*db, lvl);
  Delete(txn1, *db, 233);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, lvl);
  Scan(txn2, *db, {234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(VisibilityTest, DISABLED_TestA) {
  // only this one will be public :)
  Test1(IsolationLevel::READ_COMMITTED);
}

// NOLINTNEXTLINE
TEST(IsolationLevelTest, DISABLED_InsertTestA) {
  ExpectTwoTxn("InsertTestA.1", IsolationLevel::READ_UNCOMMITTED, IsolationLevel::READ_UNCOMMITTED, false, IS_INSERT,
               ExpectedOutcome::DirtyRead);
}

TEST(TwoInsertTest, InsertTestA) {
  size_t bustub_nft_num = 10;
  auto bustub = std::make_unique<bustub::BustubInstance>();
  auto writer = bustub::SimpleStreamWriter(std::cerr);
  // create schema
  auto schema = "CREATE TABLE nft(id int, terrier int);";
  std::cerr << "x: create schema" << std::endl;
  bustub->ExecuteSql(schema, writer);

  std::cerr << "x: nft_num=" << bustub_nft_num << std::endl;

  // initialize data
  std::cerr << "x: initialize data" << std::endl;
  std::string query = "INSERT INTO nft VALUES ";
  for (size_t i = 0; i < bustub_nft_num; i++) {
    query += fmt::format("({}, {})", i, 0);
    if (i != bustub_nft_num - 1) {
      query += ", ";
    } else {
      query += ";";
    }
  }

  {
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    auto success = bustub->ExecuteSqlTxn(query, writer, txn);
    BUSTUB_ENSURE(success, "txn not success");
    bustub->txn_manager_->Commit(txn);
    delete txn;
    if (ss.str() != fmt::format("{}\t\n", bustub_nft_num)) {
      fmt::print("unexpected result \"{}\" when insert\n", ss.str());
      exit(1);
    }
  }

  std::cerr << "x: benchmark start" << std::endl;

  size_t BUSTUB_TERRIER_THREAD = 1;
  size_t BUSTUB_TERRIER_CNT = 100;
  std::vector<std::thread> threads;
  for (size_t thread_id = 0; thread_id < BUSTUB_TERRIER_THREAD; thread_id++) {
    threads.emplace_back(std::thread([thread_id, &bustub, bustub_nft_num, BUSTUB_TERRIER_THREAD, BUSTUB_TERRIER_CNT] {
      const size_t nft_range_size = bustub_nft_num / BUSTUB_TERRIER_THREAD;
      const size_t nft_range_begin = thread_id * nft_range_size;
      const size_t nft_range_end = (thread_id + 1) * nft_range_size;
      std::random_device r;
      std::default_random_engine gen(r());
      std::uniform_int_distribution<int> nft_uniform_dist(nft_range_begin, nft_range_end - 1);
      std::uniform_int_distribution<int> terrier_uniform_dist(0, BUSTUB_TERRIER_CNT - 1);

      std::stringstream ss;
      auto writer = bustub::SimpleStreamWriter(ss, true);
      auto nft_id = nft_uniform_dist(gen);
      printf("nft id: %d\n", nft_id);
      auto terrier_id = terrier_uniform_dist(gen);
      bool txn_success = true;

      fmt::print("begin: thread {} update nft {} to terrier {}\n", thread_id, thread_id, terrier_id);

      auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);

      std::string query = fmt::format("INSERT INTO nft VALUES ({}, {})", thread_id, terrier_id);
      std::cout << query << std::endl;
      if (!bustub->ExecuteSqlTxn(query, writer, txn)) {
        txn_success = false;
      }

      if (txn_success && ss.str() != "1\t\n") {
        fmt::print("unexpected result \"{}\",\n", ss.str());
        exit(1);
      }

      if (!txn_success) {
        bustub->txn_manager_->Abort(txn);
      } else {
        std::stringstream ss;
        auto writer = bustub::SimpleStreamWriter(ss, true);
        query = "SELECT * FROM nft";
        std::cout << query << std::endl;
        if (!bustub->ExecuteSqlTxn(query, writer, txn)) {
          txn_success = false;
        }

        if (!txn_success) {
          printf("seq abort!\n");
          bustub->txn_manager_->Abort(txn);
        } else {
          CheckTableLock(txn);
          fmt::print("This is everything in your database:\n{}", ss.str());
          bustub->txn_manager_->Commit(txn);
        }
      }
      delete txn;

      fmt::print("end  : thread {} update nft {} to terrier {}\n", thread_id, nft_id, terrier_id);
    }));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  std::stringstream ss2;
  auto writer2 = bustub::SimpleStreamWriter(ss2, true);

  auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
  bool txn_success = true;

  query = "SELECT * FROM nft";
  std::cout << query << std::endl;
  if (!bustub->ExecuteSqlTxn(query, writer2, txn)) {
    txn_success = false;
  }

  if (txn_success) {
    auto all_nfts = bustub::StringUtil::Split(ss2.str(), '\n');
    auto all_nfts_integer = std::vector<int>();
    for (auto &nft : all_nfts) {
      if (nft.empty()) {
        continue;
      }
      all_nfts_integer.push_back(std::stoi(nft));
    }
    std::sort(all_nfts_integer.begin(), all_nfts_integer.end());
    // Due to how BusTub works for now, it is impossible to get more than bustub_nft_num rows, but it is possible to
    // get fewer than that number.
    fmt::print("This is everything in your database:\n{}", ss2.str());

    if (all_nfts_integer.size() != bustub_nft_num + 2) {
      fmt::print("unexpected result when verifying length. scan result: {}, total rows: {}.\n",
      all_nfts_integer.size(),
                 bustub_nft_num + 2);
      exit(1);
    }

    for (int i = 0; i < static_cast<int>(bustub_nft_num); i++) {
      if (all_nfts_integer[i] != i) {
        fmt::print("unexpected result when verifying \"{} == {}\",\n", i, all_nfts_integer[i]);
        exit(1);
      }
    }

    // query again, check repeatable read
    std::string query = "SELECT * FROM nft";
    std::cout << query << std::endl;
    std::string prev_result = ss2.str();

    std::stringstream ss3;
    auto writer3 = bustub::SimpleStreamWriter(ss3, true);
    if (!bustub->ExecuteSqlTxn(query, writer3, txn)) {
      txn_success = false;
    }

    if (txn_success) {
      if (ss3.str() != prev_result) {
        fmt::print("ERROR: non repeatable read!\n");
        if (bustub_nft_num <= 100) {
          fmt::print("This is everything in your database:\n--- previous query ---\n{}\n--- this query ---\n{}\n",
                     prev_result, ss3.str());
        }
        exit(1);
      }
      CheckTableLock(txn);
      bustub->txn_manager_->Commit(txn);
    } else {
      bustub->txn_manager_->Abort(txn);
    }
  } else {
    bustub->txn_manager_->Abort(txn);
  }
  delete txn;

  if (bustub_nft_num > 1000) {
    // if NFT num is large, sleep this thread to avoid lock contention
    std::this_thread::sleep_for(std::chrono::seconds(3));
  }
}

}  // namespace bustub
