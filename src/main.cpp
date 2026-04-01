/*
 * ================================================================
 *  Banking Transaction Processing System
 *  Week 1 + Week 2 + Week 3 + Week 4 + Week 5
 *
 *  OS Concepts:
 *  Week 1: Threads, Mutex, Condition Variable,
 *          Deadlock Prevention, Process Scheduling
 *  Week 2: Semaphore, IPC Foundation, MySQL Persistence
 *  Week 3: Producer-Consumer, Bounded Buffer, Dual Semaphores
 *  Week 4: Readers-Writers, Read-Write Lock, Writer Priority
 *  Week 5: Shared Memory, Message Queue, Pipes
 * ================================================================
 */

#include <iostream>
#include <memory>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <iomanip>
#include <mutex>
#include <cstring>
#include <sstream>

#include "ConnectionPool.h"
#include "Account.h"
#include "Transaction.h"
#include "ThreadPool.h"
#include "BoundedQueue.h"
#include "ReadWriteLock.h"
#include "IPCManager.h"

// ── Terminal colors ──────────────────────────────────────────
#define RESET   "\033[0m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define YELLOW  "\033[33m"
#define CYAN    "\033[36m"
#define MAGENTA "\033[35m"
#define BOLD    "\033[1m"
#define CLEAR   "\033[2J\033[H"

// ── DB CONFIG ────────────────────────────────────────────────
const std::string DB_HOST     = "127.0.0.1";
const std::string DB_USER     = "kimay";
const std::string DB_PASSWORD = "kimaya@1411";
const std::string DB_NAME     = "banking_system";
const int         POOL_SIZE   = 5;

extern bool DB_LOGGING_ENABLED;

std::atomic<int> g_success{0};
std::atomic<int> g_failed{0};
std::atomic<int> g_deadlock_prevented{0};

// ─────────────────────────────────────────────────────────────
void printHeader() {
    std::cout << CLEAR << BOLD << CYAN;
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║        BANKING TRANSACTION PROCESSING SYSTEM                ║\n";
    std::cout << "║        Week 1+2+3+4+5  |  16 OS Concepts                   ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    std::cout << RESET << "\n";
}

void printSection(const std::string& title) {
    std::cout << "\n" << BOLD << YELLOW;
    std::cout << "┌─────────────────────────────────────────────────────┐\n";
    std::cout << "│  " << std::left << std::setw(51) << title << "│\n";
    std::cout << "└─────────────────────────────────────────────────────┘\n";
    std::cout << RESET;
}

void showBalances(std::vector<std::shared_ptr<Account>>& accounts) {
    std::cout << "\n  ┌────────────┬──────────┬──────────────┐\n";
    std::cout << "  │ Account    │ Type     │ Balance      │\n";
    std::cout << "  ├────────────┼──────────┼──────────────┤\n";
    for (auto& a : accounts) a->displayInfo();
    std::cout << "  └────────────┴──────────┴──────────────┘\n";
}

// ─────────────────────────────────────────────────────────────
//  DEMO 1 — Persistence (Week 2)
// ─────────────────────────────────────────────────────────────
void demoPersistence(std::vector<std::shared_ptr<Account>>& accounts,
                     ConnectionPool& pool) {
    printSection("DEMO 1: DB PERSISTENCE  (Week 2)");
    DB_LOGGING_ENABLED = false;

    std::cout << GREEN << "\n  Balances loaded FROM MySQL on startup:\n" << RESET;
    showBalances(accounts);

    std::cout << "\n  Depositing $500 to ACC-001, $300 to ACC-002...\n";
    Transaction t1(1, TxType::DEPOSIT, nullptr, accounts[0], 500.0, &pool);
    Transaction t2(2, TxType::DEPOSIT, nullptr, accounts[1], 300.0, &pool);
    t1.execute();
    t2.execute();

    std::cout << "  Saving updated balances to MySQL...\n";
    for (auto& acc : accounts) {
        try {
            auto conn = pool.acquire();
            conn->updateBalance(acc->getAccountNumber(), acc->getBalance());
            pool.release(conn);
        } catch (...) {}
    }

    std::cout << GREEN << "\n  Updated balances (saved to MySQL):\n" << RESET;
    showBalances(accounts);
    std::cout << CYAN << "\n  Restart -> balances load from these values.\n"
              << "  Week 1 reset every time. That is persistence.\n" << RESET;

    std::cout << "\n  Press Enter to continue...";
    std::cin.get();
}

// ─────────────────────────────────────────────────────────────
//  DEMO 2 — Semaphore (Week 2)
// ─────────────────────────────────────────────────────────────
void demoSemaphore(ConnectionPool& pool) {
    printSection("DEMO 2: SEMAPHORE  (Week 2)");

    int launch = POOL_SIZE + 3;
    std::cout << "\n  Pool size   : " << POOL_SIZE << " connections\n";
    std::cout << "  Launching   : " << launch << " threads simultaneously\n";
    std::cout << "  Last 3 will : BLOCK at sem_wait() until others release\n\n";

    std::vector<std::thread> threads;
    std::mutex print_m;

    for (int i = 0; i < launch; i++) {
        threads.emplace_back([&pool, &print_m, i]() {
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << "  [Thread-" << i << "] Requesting connection...\n"; }
            auto conn = pool.acquire();
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << GREEN << "  [Thread-" << i << "] Got connection! Working...\n" << RESET; }
            std::this_thread::sleep_for(std::chrono::milliseconds(400));
            pool.release(conn);
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << "  [Thread-" << i << "] Released connection.\n"; }
        });
    }
    for (auto& t : threads) t.join();

    pool.printStats();
    std::cout << GREEN << "\n  Semaphore ensured max " << POOL_SIZE
              << " concurrent DB connections at any time.\n" << RESET;
    std::cout << "\n  Press Enter to continue...";
    std::cin.get();
}

// ─────────────────────────────────────────────────────────────
//  DEMO 3 — Deadlock Prevention (Week 1)
// ─────────────────────────────────────────────────────────────
void demoDeadlock(std::vector<std::shared_ptr<Account>>& accounts,
                  ConnectionPool& pool) {
    printSection("DEMO 3: DEADLOCK PREVENTION  (Week 1)");

    std::cout << "\n  20 cross-transfers ACC-001 <-> ACC-002 simultaneously\n";
    std::cout << "  Without fix : circular wait -> deadlock forever\n";
    std::cout << "  With fix    : always lock lower ID first -> no deadlock\n\n";

    ThreadPool tp(4);
    g_success = 0; g_failed = 0;

    for (int i = 0; i < 20; i++) {
        if (i % 2 == 0) {
            tp.enqueue([&, i]() {
                Transaction t(i, TxType::TRANSFER, accounts[0], accounts[1], 50.0, &pool);
                t.execute() ? g_success++ : g_failed++;
                g_deadlock_prevented++;
            });
        } else {
            tp.enqueue([&, i]() {
                Transaction t(i, TxType::TRANSFER, accounts[1], accounts[0], 50.0, &pool);
                t.execute() ? g_success++ : g_failed++;
            });
        }
    }
    tp.waitAll();

    std::cout << GREEN << "  Done. Zero deadlocks.\n" << RESET;
    std::cout << "  Success: " << g_success
              << "  Failed (low balance): " << g_failed << "\n";
    showBalances(accounts);
    std::cout << "\n  Press Enter to continue...";
    std::cin.get();
}

// ─────────────────────────────────────────────────────────────
//  DEMO 4 — Mutex (Week 1)
// ─────────────────────────────────────────────────────────────
void demoMutex(std::vector<std::shared_ptr<Account>>& accounts,
               ConnectionPool& pool) {
    printSection("DEMO 4: MUTEX - RACE CONDITION PREVENTION  (Week 1)");

    double before = accounts[2]->getBalance();
    std::cout << "\n  50 threads deposit $10 to ACC-003 simultaneously\n";
    std::cout << "  Without mutex : wrong total (race condition)\n";
    std::cout << "  With mutex    : correct total guaranteed\n\n";
    std::cout << "  Balance before : $" << std::fixed << std::setprecision(2) << before << "\n";
    std::cout << "  Expected after : $" << (before + 500.0) << "\n\n";

    ThreadPool tp(8);
    for (int i = 0; i < 50; i++) {
        tp.enqueue([&, i]() {
            Transaction t(i, TxType::DEPOSIT, nullptr, accounts[2], 10.0, &pool);
            t.execute();
        });
    }
    tp.waitAll();

    double after = accounts[2]->getBalance();
    std::cout << "  Balance after  : $" << after << "\n";
    if (std::abs(after - (before + 500.0)) < 0.01)
        std::cout << GREEN << "  CORRECT - mutex prevented all race conditions\n" << RESET;
    else
        std::cout << RED << "  Race condition! Expected $"
                  << (before+500) << " got $" << after << "\n" << RESET;

    std::cout << "\n  Press Enter to continue...";
    std::cin.get();
}

// ─────────────────────────────────────────────────────────────
//  DEMO 5 — Producer-Consumer (Week 3)
// ─────────────────────────────────────────────────────────────
void demoProducerConsumer(std::vector<std::shared_ptr<Account>>& accounts,
                           ConnectionPool& pool) {
    printSection("DEMO 5: PRODUCER-CONSUMER  (Week 3)");

    const int QUEUE_SIZE = 8;
    const int TOTAL      = 24;

    std::cout << "\n  Bounded Queue capacity : " << QUEUE_SIZE << " slots\n";
    std::cout << "  Producers              : 3 (ATM, MOBILE, WEB)\n";
    std::cout << "  Consumers              : 3 worker threads\n";
    std::cout << "  Total requests         : " << TOTAL << "\n\n";
    std::cout << "  When queue fills -> producers BLOCK (sem_wait empty_slots)\n";
    std::cout << "  When queue empty -> consumers BLOCK (sem_wait full_slots)\n\n";

    BoundedQueue bq(QUEUE_SIZE);
    std::atomic<int> consumed{0};
    std::atomic<bool> done_producing{false};
    std::mutex result_mutex;
    int cons_success = 0, cons_failed = 0;

    std::vector<std::string> channels = {"ATM", "MOBILE", "WEB"};
    std::vector<std::string> accs = {"ACC-001","ACC-002","ACC-003","ACC-004","ACC-005"};

    std::vector<std::thread> consumers;
    for (int c = 0; c < 3; c++) {
        consumers.emplace_back([&, c]() {
            while (true) {
                TransactionRequest req;
                if (!bq.tryConsume(req)) {
                    if (done_producing && bq.getCurrentSize() == 0) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }
                int to_idx = -1, from_idx = -1;
                for (int i = 0; i < (int)accounts.size(); i++) {
                    if (accounts[i]->getAccountNumber() == req.to_acc)   to_idx   = i;
                    if (accounts[i]->getAccountNumber() == req.from_acc) from_idx = i;
                }
                bool ok = false;
                if (req.type == "DEPOSIT" && to_idx >= 0) {
                    Transaction t(req.id, TxType::DEPOSIT, nullptr, accounts[to_idx], req.amount, &pool);
                    ok = t.execute();
                } else if (req.type == "TRANSFER" && from_idx >= 0 && to_idx >= 0) {
                    Transaction t(req.id, TxType::TRANSFER, accounts[from_idx], accounts[to_idx], req.amount, &pool);
                    ok = t.execute();
                }
                { std::lock_guard<std::mutex> lk(result_mutex);
                  if (ok) cons_success++; else cons_failed++; }
                consumed++;
                std::cout << "  [Consumer-" << c << "] " << req.channel
                          << " " << req.type << " $" << req.amount
                          << (ok ? GREEN " OK" RESET : RED " FAIL" RESET) << "\n";
            }
        });
    }

    std::vector<std::thread> producers;
    for (int p = 0; p < 3; p++) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < 8; i++) {
                TransactionRequest req;
                req.id       = p * 100 + i;
                req.channel  = channels[p];
                req.type     = (i % 2 == 0) ? "DEPOSIT" : "TRANSFER";
                req.from_acc = accs[i % accs.size()];
                req.to_acc   = accs[(i+1) % accs.size()];
                req.amount   = 50.0 + (i % 4) * 25.0;
                std::cout << "  [" << channels[p] << "] -> queue size:"
                          << bq.getCurrentSize() << "/" << QUEUE_SIZE << "\n";
                bq.produce(req);
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        });
    }

    for (auto& p : producers) p.join();
    done_producing = true;
    for (int w = 0; w < 500 && consumed < TOTAL; w++)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    for (auto& c : consumers) if (c.joinable()) c.detach();

    bq.printStats();
    std::cout << GREEN << "\n  Producer-Consumer complete! Processed: " << consumed << "\n" << RESET;
    showBalances(accounts);
    std::cout << "\n  Press Enter to continue...";
    std::cin.get();
}

// ─────────────────────────────────────────────────────────────
//  DEMO 6 — Readers-Writers (Week 4)
// ─────────────────────────────────────────────────────────────
void demoReadersWriters(std::vector<std::shared_ptr<Account>>& accounts) {
    printSection("DEMO 6: READERS-WRITERS PROBLEM  (Week 4)");

    std::cout << "\n  Scenario: Balance inquiry desk at peak hour\n";
    std::cout << "  READERS : Multiple threads check balances simultaneously\n";
    std::cout << "  WRITERS : Transfer thread needs exclusive access\n\n";
    std::cout << "  Rule: Many readers OR one writer - never both\n\n";

    ReadWriteLock rwl;
    std::mutex print_m;

    std::vector<double> balances(accounts.size());
    for (int i = 0; i < (int)accounts.size(); i++)
        balances[i] = accounts[i]->getBalance();

    std::vector<std::thread> readers;
    for (int r = 0; r < 6; r++) {
        readers.emplace_back([&, r]() {
            for (int iter = 0; iter < 3; iter++) {
                { std::lock_guard<std::mutex> lk(print_m);
                  std::cout << CYAN << "  [Reader-" << r << "] Acquiring read lock...\n" << RESET; }
                ReadGuard rg(rwl);
                { std::lock_guard<std::mutex> lk(print_m);
                  std::cout << GREEN << "  [Reader-" << r << "] Reading (shared access OK)\n" << RESET; }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
                double total = 0;
                for (auto b : balances) total += b;
                { std::lock_guard<std::mutex> lk(print_m);
                  std::cout << "  [Reader-" << r << "] Total: $"
                            << std::fixed << std::setprecision(2) << total << " - releasing\n"; }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        });
    }

    std::vector<std::thread> writers;
    for (int w = 0; w < 2; w++) {
        writers.emplace_back([&, w]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(100 + w * 200));
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << YELLOW << "\n  [Writer-" << w << "] Requesting WRITE lock...\n" << RESET; }
            WriteGuard wg(rwl);
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << RED << "  [Writer-" << w << "] EXCLUSIVE access - transferring $500\n" << RESET;
              std::cout << "  [Writer-" << w << "] All readers BLOCKED\n"; }
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            balances[w] -= 500.0;
            balances[(w+1) % balances.size()] += 500.0;
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << GREEN << "  [Writer-" << w << "] Done - releasing write lock\n\n" << RESET; }
        });
    }

    for (auto& r : readers) r.join();
    for (auto& w : writers) w.join();

    rwl.printStats();
    std::cout << GREEN << "\n  Readers-Writers complete!\n" << RESET;
    std::cout << "  Multiple readers ran simultaneously\n";
    std::cout << "  Writers got exclusive access - zero corruption\n";
    std::cout << "\n  Press Enter to continue...";
    std::cin.get();
}

// ─────────────────────────────────────────────────────────────
//  DEMO 7 — IPC: Shared Memory + Message Queue + Pipes (Week 5)
// ─────────────────────────────────────────────────────────────
void demoIPC(std::vector<std::shared_ptr<Account>>& accounts,
             ConnectionPool& pool) {
    printSection("DEMO 7: IPC - SHARED MEM + MSG QUEUE + PIPES  (Week 5)");

    std::cout << "\n  3 IPC mechanisms demonstrated:\n";
    std::cout << "  1. Shared Memory  - fraud alert flag visible to all threads\n";
    std::cout << "  2. Message Queue  - transaction notifications to audit thread\n";
    std::cout << "  3. Pipe           - results streamed to audit logger\n\n";

    IPCManager ipc;
    std::cout << BOLD << "  [INIT] Setting up IPC mechanisms...\n" << RESET;
    bool shm_ok  = ipc.initSharedMemory();
    bool mq_ok   = ipc.initMessageQueue();
    bool pipe_ok = ipc.initPipe();

    if (!shm_ok || !mq_ok || !pipe_ok) {
        std::cout << RED << "  IPC init failed\n" << RESET;
        return;
    }
    std::cout << GREEN << "  All 3 IPC mechanisms ready!\n\n" << RESET;

    std::mutex print_m;

    // ── Part 1: Shared Memory ─────────────────────────────────
    std::cout << BOLD << CYAN << "  --- Part 1: SHARED MEMORY (Fraud Detection) ---\n" << RESET;
    std::cout << "  10 threads share ONE fraud flag via OS shared memory\n\n";

    std::vector<std::thread> txn_threads;
    for (int i = 0; i < 10; i++) {
        txn_threads.emplace_back([&, i]() {
            double amount = 100.0 + i * 150.0;
            std::string acc = accounts[i % accounts.size()]->getAccountNumber();
            if (amount > 1000.0) {
                ipc.setFraudAlert(acc, amount);
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << RED << "  [Thread-" << i << "] FRAUD ALERT -> SHM: "
                          << acc << " $" << amount << "\n" << RESET;
            } else {
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << GREEN << "  [Thread-" << i << "] Normal: "
                          << acc << " $" << amount << "\n" << RESET;
            }
            if (ipc.isFraudAlert()) {
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << YELLOW << "  [Thread-" << i << "] Reads SHM: FRAUD DETECTED\n" << RESET;
            }
        });
    }
    for (auto& t : txn_threads) t.join();

    SharedFraudData* data = ipc.getSharedData();
    std::cout << "\n  [SHM RESULT] Suspicious: " << data->suspicious_count
              << " | Flagged: $" << data->total_flagged << "\n";
    ipc.clearFraudAlert();

    std::cout << "\n  Press Enter for Part 2...";
    std::cin.get();

    // ── Part 2: Message Queue ─────────────────────────────────
    std::cout << BOLD << CYAN << "\n  --- Part 2: MESSAGE QUEUE (Audit Notifications) ---\n" << RESET;
    std::cout << "  Producers send msgs | Audit thread receives\n\n";

    std::thread audit_thread([&]() {
        int received = 0;
        while (received < 6) {
            TransactionMessage msg;
            if (ipc.receiveMessage(msg)) {
                received++;
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << MAGENTA << "  [AUDIT] " << msg.account
                          << " " << msg.type << " $" << msg.amount
                          << (msg.status ? " OK" : " FAIL") << "\n" << RESET;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });

    std::string types[] = {"DEPOSIT", "WITHDRAW", "TRANSFER"};
    for (int i = 0; i < 6; i++) {
        std::string acc = accounts[i % accounts.size()]->getAccountNumber();
        std::string type = types[i % 3];
        double amt = 200.0 + i * 50.0;
        std::cout << "  [Producer-" << i << "] Sending: " << acc
                  << " " << type << " $" << amt << "\n";
        ipc.sendMessage(acc, type, amt, true);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    audit_thread.join();
    std::cout << GREEN << "\n  Message Queue: all 6 notifications delivered!\n" << RESET;

    std::cout << "\n  Press Enter for Part 3...";
    std::cin.get();

    // ── Part 3: Pipe ──────────────────────────────────────────
    std::cout << BOLD << CYAN << "\n  --- Part 3: PIPES (Audit Logger) ---\n" << RESET;
    std::cout << "  Writer -> pipe fd[1] | Reader reads from fd[0]\n\n";

    std::thread pipe_reader([&]() {
        for (int i = 0; i < 5; i++) {
            std::string data = ipc.readFromPipe();
            std::lock_guard<std::mutex> lk(print_m);
            std::cout << CYAN << "  [AUDIT LOGGER] " << data << RESET;
        }
    });

    std::thread pipe_writer([&]() {
        std::string txns[] = {
            "ACC-001 DEPOSIT $500 OK",
            "ACC-002 WITHDRAW $200 OK",
            "ACC-003 TRANSFER $750 OK",
            "ACC-004 DEPOSIT $1000 OK",
            "ACC-005 WITHDRAW $300 FAIL"
        };
        for (auto& txn : txns) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << "  [WRITER] Pipe write: " << txn << "\n"; }
            ipc.writeToPipe(txn);
        }
    });

    pipe_writer.join();
    pipe_reader.join();
    ipc.closePipe();

    std::cout << GREEN << "\n  Pipe: all 5 audit records streamed!\n" << RESET;
    ipc.printStats();
    std::cout << GREEN << "\n  IPC Demo complete! 3 mechanisms working together.\n" << RESET;
    std::cout << "\n  Press Enter to continue...";
    std::cin.get();
}

// ─────────────────────────────────────────────────────────────
//  DEMO 8 — Full Stress Test
// ─────────────────────────────────────────────────────────────
void demoStressTest(std::vector<std::shared_ptr<Account>>& accounts,
                    ConnectionPool& pool) {
    printSection("DEMO 8: FULL STRESS TEST  (200 transactions)");

    const int TOTAL = 200;
    g_success = 0; g_failed = 0;

    ThreadPool tp(4);
    auto start = std::chrono::system_clock::now();

    std::thread submitter([&]() {
        for (int i = 0; i < TOTAL; i++) {
            int type = i % 3;
            int a = i % (int)accounts.size();
            int b = (i + 1) % (int)accounts.size();
            if (type == 0) {
                tp.enqueue([&, a, i]() {
                    Transaction t(i, TxType::DEPOSIT, nullptr, accounts[a], 100.0, &pool);
                    t.execute() ? g_success++ : g_failed++;
                });
            } else if (type == 1) {
                tp.enqueue([&, a, i]() {
                    Transaction t(i, TxType::WITHDRAW, accounts[a], nullptr, 50.0, &pool);
                    t.execute() ? g_success++ : g_failed++;
                });
            } else {
                tp.enqueue([&, a, b, i]() {
                    Transaction t(i, TxType::TRANSFER, accounts[a], accounts[b], 75.0, &pool);
                    t.execute() ? g_success++ : g_failed++;
                });
            }
        }
    });

    while ((g_success + g_failed) < TOTAL) {
        int done = g_success + g_failed;
        int filled = done * 40 / TOTAL;
        std::cout << "\r  [";
        for (int i = 0; i < 40; i++) std::cout << (i < filled ? "#" : ".");
        std::cout << "]  " << done << "/" << TOTAL
                  << "  OK:" << g_success << " FAIL:" << g_failed
                  << "  threads:" << tp.getActive()
                  << "   " << std::flush;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    submitter.join();
    tp.waitAll();

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now() - start).count();

    std::cout << "\n\n" << BOLD << "  Done!\n" << RESET;
    std::cout << "  " << GREEN << "Success: " << g_success << RESET
              << "   " << RED << "Failed: " << g_failed << RESET << "\n";
    std::cout << "  Time: " << elapsed << "s";
    if (elapsed > 0)
        std::cout << "  |  Throughput: " << (TOTAL / elapsed) << " txn/sec";
    std::cout << "\n";
    showBalances(accounts);
}

// ─────────────────────────────────────────────────────────────
//  SUMMARY
// ─────────────────────────────────────────────────────────────
void printSummary(ConnectionPool& pool) {
    printSection("OS CONCEPTS SUMMARY  --  Week 1 + 2 + 3 + 4 + 5");

    std::cout << "\n  Week 1:\n";
    std::cout << "  " << GREEN << ">" << RESET << " Threads            -- 4 worker threads in ThreadPool\n";
    std::cout << "  " << GREEN << ">" << RESET << " Mutex              -- account_mutex in every deposit/withdraw\n";
    std::cout << "  " << GREEN << ">" << RESET << " Condition Variable -- workers sleep idle, wake on new task\n";
    std::cout << "  " << GREEN << ">" << RESET << " Deadlock Prevention-- transfers lock lower account ID first\n";
    std::cout << "  " << GREEN << ">" << RESET << " Process Scheduling -- thread pool FIFO task distribution\n";

    std::cout << "\n  Week 2:\n";
    std::cout << "  " << CYAN << ">" << RESET << " Semaphore          -- sem_wait/sem_post limits DB connections\n";
    std::cout << "  " << CYAN << ">" << RESET << " IPC Foundation     -- shared pool resource across threads\n";
    std::cout << "  " << CYAN << ">" << RESET << " Persistence        -- MySQL saves all state across restarts\n";

    std::cout << "\n  Week 3:\n";
    std::cout << "  " << MAGENTA << ">" << RESET << " Producer-Consumer  -- ATM/Mobile/Web produce, workers consume\n";
    std::cout << "  " << MAGENTA << ">" << RESET << " Bounded Buffer     -- fixed queue prevents memory overflow\n";
    std::cout << "  " << MAGENTA << ">" << RESET << " Dual Semaphores    -- empty_slots + full_slots coordination\n";

    std::cout << "\n  Week 4:\n";
    std::cout << "  " << YELLOW << ">" << RESET << " Readers-Writers    -- multiple readers OR one exclusive writer\n";
    std::cout << "  " << YELLOW << ">" << RESET << " Read-Write Lock    -- write_sem blocks writers, coordinates readers\n";
    std::cout << "  " << YELLOW << ">" << RESET << " Writer Priority    -- writers wait for ALL readers to finish\n";

    std::cout << "\n  Week 5:\n";
    std::cout << "  " << RED << ">" << RESET << " Shared Memory      -- shmget/shmat fraud alert across threads\n";
    std::cout << "  " << RED << ">" << RESET << " Message Queue      -- msgsnd/msgrcv audit notifications\n";
    std::cout << "  " << RED << ">" << RESET << " Pipes              -- pipe()/read()/write() audit logger\n";

    std::cout << "\n  " << BOLD << "Total: 16 OS Concepts demonstrated!\n" << RESET;

    pool.printStats();

    std::cout << "\n" << BOLD << GREEN
              << "  Project COMPLETE! Weeks 1-5 done.\n"
              << RESET << "\n";
}

// ─────────────────────────────────────────────────────────────
//  MAIN
// ─────────────────────────────────────────────────────────────
int main() {
    printHeader();

    DB_LOGGING_ENABLED = false;

    ConnectionPool pool(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, POOL_SIZE);

    std::cout << BOLD << "\n[STARTUP] Loading accounts from MySQL...\n" << RESET;
    std::vector<std::shared_ptr<Account>> accounts = {
        std::make_shared<Account>(1, "ACC-001", "SAVINGS",  10000.0, &pool),
        std::make_shared<Account>(2, "ACC-002", "CURRENT",   5000.0, &pool),
        std::make_shared<Account>(3, "ACC-003", "VIP",      50000.0, &pool),
        std::make_shared<Account>(4, "ACC-004", "SAVINGS",   8000.0, &pool),
        std::make_shared<Account>(5, "ACC-005", "CURRENT",  12000.0, &pool),
    };
    std::cout << GREEN << "[STARTUP] All accounts ready.\n" << RESET;

    std::cout << "\n  Press Enter to run all demos...";
    std::cin.get();

    demoPersistence(accounts, pool);       // Demo 1 - Week 2
    demoSemaphore(pool);                   // Demo 2 - Week 2
    demoDeadlock(accounts, pool);          // Demo 3 - Week 1
    demoMutex(accounts, pool);             // Demo 4 - Week 1
    demoProducerConsumer(accounts, pool);  // Demo 5 - Week 3
    demoReadersWriters(accounts);          // Demo 6 - Week 4
    demoIPC(accounts, pool);              // Demo 7 - Week 5
    demoStressTest(accounts, pool);        // Demo 8 - Final

    printSummary(pool);

    return 0;
}
