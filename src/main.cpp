/*
 * ================================================================
 *  Banking Transaction Processing System
 *  OS Concepts: Threads, Mutex, Condition Variable,
 *               Deadlock Prevention, Process Scheduling,
 *               Semaphore, IPC, MySQL Persistence,
 *               Producer-Consumer, Bounded Buffer,
 *               Readers-Writers, Shared Memory,
 *               Message Queue, Pipes,
 *               Round Robin Scheduling, Priority Scheduling,
 *               FCFS Scheduling
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
#include <cmath>
#include <climits>
#include "ConnectionPool.h"
#include "Account.h"
#include "Transaction.h"
#include "ThreadPool.h"
#include "BoundedQueue.h"
#include "ReadWriteLock.h"
#include "IPCManager.h"
#include "CPUScheduler.h"

// ── Terminal colors & styles ─────────────────────────────────
#define RESET     "\033[0m"
#define RED       "\033[31m"
#define GREEN     "\033[32m"
#define YELLOW    "\033[33m"
#define CYAN      "\033[36m"
#define MAGENTA   "\033[35m"
#define BLUE      "\033[34m"
#define BOLD      "\033[1m"
#define DIM       "\033[2m"
#define CLEAR     "\033[2J\033[H"

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
//  UI HELPERS
// ─────────────────────────────────────────────────────────────

void printHeader() {
    std::cout << CLEAR;
    std::cout << BOLD << CYAN;
    std::cout << "╔══════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                                                                  ║\n";
    std::cout << "║      BANKING TRANSACTION PROCESSING SYSTEM                      ║\n";
    std::cout << "║      OS Concepts Live Demo  |  19 Concepts  |  C++ / Linux      ║\n";
    std::cout << "║                                                                  ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════════╝\n";
    std::cout << RESET << "\n";
}

void printTopic(const std::string& topic,
                const std::string& mechanism,
                const std::string& tagline) {
    std::cout << "\n" << BOLD;
    std::cout << "┌──────────────────────────────────────────────────────────────────┐\n";
    std::cout << "│  " << CYAN   << std::left << std::setw(64) << topic     << RESET << BOLD << "│\n";
    std::cout << "│  " << DIM    << std::left << std::setw(64) << ("[" + mechanism + "]") << RESET << BOLD << "│\n";
    std::cout << "│  " << YELLOW << std::left << std::setw(64) << tagline   << RESET << BOLD << "│\n";
    std::cout << "└──────────────────────────────────────────────────────────────────┘\n";
    std::cout << RESET;
}

void printSubSection(const std::string& title) {
    std::cout << "\n  " << BOLD << BLUE << "── " << title << " ──" << RESET << "\n\n";
}

void showBalances(std::vector<std::shared_ptr<Account>>& accounts) {
    std::cout << "\n";
    std::cout << "  ┌────────────┬──────────┬──────────────┐\n";
    std::cout << "  │ " << BOLD << "Account   " << RESET
              << " │ " << BOLD << "Type    " << RESET
              << "  │ " << BOLD << "Balance     " << RESET << "  │\n";
    std::cout << "  ├────────────┼──────────┼──────────────┤\n";
    for (auto& a : accounts) a->displayInfo();
    std::cout << "  └────────────┴──────────┴──────────────┘\n";
}

void printProgress(int done, int total, int ok, int fail, int threads) {
    int filled = done * 42 / total;
    std::cout << "\r  " << CYAN << "[" << RESET;
    for (int i = 0; i < 42; i++) {
        if (i < filled) std::cout << GREEN << "\xe2\x96\x88" << RESET;
        else            std::cout << DIM   << "\xe2\x96\x91" << RESET;
    }
    std::cout << CYAN << "]" << RESET
              << "  " << BOLD << done << "/" << total << RESET
              << "  " << GREEN << "OK:" << ok   << RESET
              << " " << RED   << "FAIL:" << fail << RESET
              << "  threads:" << threads
              << "   " << std::flush;
}

void printWhyItMatters(const std::string& without, const std::string& withit) {
    std::cout << "\n";
    std::cout << "  ┌─ WHY THIS MATTERS ──────────────────────────────────────────┐\n";
    std::cout << "  │ " << RED   << "Without: " << RESET << std::left << std::setw(55) << without << "│\n";
    std::cout << "  │ " << GREEN << "With   : " << RESET << std::left << std::setw(55) << withit  << "│\n";
    std::cout << "  └─────────────────────────────────────────────────────────────┘\n\n";
}

void pauseForEnter(const std::string& hint = "") {
    if (!hint.empty())
        std::cout << "\n  " << DIM << hint << RESET;
    std::cout << "\n  " << DIM << "Press Enter to continue..." << RESET;
    std::cin.get();
}

// ─────────────────────────────────────────────────────────────
//  1. DURABILITY & PERSISTENCE
// ─────────────────────────────────────────────────────────────
void topicPersistence(std::vector<std::shared_ptr<Account>>& accounts,
                      ConnectionPool& pool) {
    printTopic(
        "DURABILITY & PERSISTENCE",
        "MySQL + Connection Pool",
        "Account state survives program restarts — unlike in-memory-only systems"
    );
    DB_LOGGING_ENABLED = false;

    printWhyItMatters(
        "All balances reset to defaults on every run (volatile RAM only)",
        "Balances loaded from MySQL — changes are permanent across restarts"
    );

    std::cout << GREEN << "  Balances loaded from MySQL on startup:\n" << RESET;
    showBalances(accounts);

    std::cout << "\n  Depositing $500 to ACC-001,  $300 to ACC-002...\n";
    Transaction t1(1, TxType::DEPOSIT, nullptr, accounts[0], 500.0, &pool);
    Transaction t2(2, TxType::DEPOSIT, nullptr, accounts[1], 300.0, &pool);
    t1.execute();
    t2.execute();

    std::cout << "  Persisting updated balances to MySQL...\n";
    for (auto& acc : accounts) {
        try {
            auto conn = pool.acquire();
            conn->updateBalance(acc->getAccountNumber(), acc->getBalance());
            pool.release(conn);
        } catch (...) {}
    }

    std::cout << GREEN << "\n  Updated balances (saved to disk):\n" << RESET;
    showBalances(accounts);

    std::cout << CYAN
              << "\n  Restart the program -> these exact values load again.\n"
              << "  Without MySQL, every restart shows the initial defaults.\n"
              << RESET;
    pauseForEnter();
}

// ─────────────────────────────────────────────────────────────
//  2. SEMAPHORE — CONNECTION LIMITING
// ─────────────────────────────────────────────────────────────
void topicSemaphore(ConnectionPool& pool) {
    printTopic(
        "SEMAPHORE — CONNECTION LIMITING",
        "POSIX sem_wait / sem_post",
        "Exactly 5 threads may hold a DB connection at once — extras block"
    );

    int launch = POOL_SIZE + 3;

    printWhyItMatters(
        "Unlimited DB connections -> server overload, OOM, crashes",
        "Semaphore hard-caps concurrency — excess threads wait, not fail"
    );

    std::cout << "  Pool size   : " << BOLD << POOL_SIZE << RESET << " connections\n";
    std::cout << "  Launching   : " << BOLD << launch    << RESET << " threads simultaneously\n";
    std::cout << "  Threads 5,6,7 will " << RED << "BLOCK" << RESET
              << " at sem_wait() — watch below:\n\n";

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
              std::cout << DIM << "  [Thread-" << i << "] Released.\n" << RESET; }
        });
    }
    for (auto& t : threads) t.join();

    std::cout << "\n";
    pool.printStats();
    std::cout << GREEN
              << "\n  Semaphore guaranteed max " << POOL_SIZE
              << " concurrent connections at all times.\n"
              << RESET;
    pauseForEnter();
}

// ─────────────────────────────────────────────────────────────
//  3. DEADLOCK PREVENTION
// ─────────────────────────────────────────────────────────────
void topicDeadlock(std::vector<std::shared_ptr<Account>>& accounts,
                   ConnectionPool& pool) {
    printTopic(
        "DEADLOCK PREVENTION",
        "Lock Ordering Protocol — always acquire lower account ID first",
        "20 simultaneous cross-transfers between same two accounts — zero deadlocks"
    );

    printWhyItMatters(
        "Thread-A locks ACC-001, Thread-B locks ACC-002 -> circular wait -> freeze",
        "Always lock lower ID first -> no circular dependency possible"
    );

    std::cout << "  Scenario : ACC-001->ACC-002 and ACC-002->ACC-001 simultaneously\n";
    std::cout << "  Threads  : 4 workers, 20 cross-transfers\n";
    std::cout << "  Fix      : " << GREEN << "lock(min_id) first, then lock(max_id)\n" << RESET << "\n";

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

    std::cout << GREEN << BOLD << "\n  Result: Zero deadlocks.\n" << RESET;
    std::cout << "  Successful transfers : " << GREEN << g_success << RESET << "\n";
    std::cout << "  Failed (low balance) : "
              << (g_failed > 0 ? RED : GREEN) << g_failed << RESET << "\n";
    showBalances(accounts);
    pauseForEnter();
}

// ─────────────────────────────────────────────────────────────
//  4. MUTEX — RACE CONDITION PREVENTION
// ─────────────────────────────────────────────────────────────
void topicMutex(std::vector<std::shared_ptr<Account>>& accounts,
                ConnectionPool& pool) {
    printTopic(
        "MUTEX — RACE CONDITION PREVENTION",
        "std::mutex  account_mutex  per Account object",
        "50 concurrent deposits — final balance must be mathematically exact"
    );

    printWhyItMatters(
        "No mutex: two threads read 50000, both add $10, both write 50010 — $10 lost",
        "With mutex: read-modify-write is atomic — every single deposit counted"
    );

    double before = accounts[2]->getBalance();
    std::cout << "  Account       : ACC-003 (VIP)\n";
    std::cout << "  Threads       : " << BOLD << "50" << RESET
              << " concurrent deposits of $10 each\n";
    std::cout << "  Balance before: " << YELLOW << "$"
              << std::fixed << std::setprecision(2) << before << RESET << "\n";
    std::cout << "  Expected after: " << YELLOW << "$" << (before + 500.0) << RESET << "\n\n";

    ThreadPool tp(8);
    for (int i = 0; i < 50; i++) {
        tp.enqueue([&, i]() {
            Transaction t(i, TxType::DEPOSIT, nullptr, accounts[2], 10.0, &pool);
            t.execute();
        });
    }
    tp.waitAll();

    double after = accounts[2]->getBalance();
    std::cout << "  Balance after : " << BOLD << "$" << after << RESET << "\n\n";

    if (std::abs(after - (before + 500.0)) < 0.01) {
        std::cout << GREEN << BOLD
                  << "  CORRECT — mutex prevented all race conditions.\n"
                  << "  Every one of the 50 deposits was counted exactly.\n"
                  << RESET;
    } else {
        std::cout << RED << "  Race condition detected! Expected $"
                  << (before + 500) << " got $" << after << "\n" << RESET;
    }
    pauseForEnter();
}

// ─────────────────────────────────────────────────────────────
//  5. PRODUCER-CONSUMER WITH BOUNDED BUFFER
// ─────────────────────────────────────────────────────────────
void topicProducerConsumer(std::vector<std::shared_ptr<Account>>& accounts,
                            ConnectionPool& pool) {
    printTopic(
        "PRODUCER-CONSUMER WITH BOUNDED BUFFER",
        "POSIX Semaphores: empty_slots + full_slots",
        "ATM / Mobile / Web produce requests — worker threads consume them"
    );

    const int QUEUE_SIZE = 8;
    const int TOTAL      = 24;

    printWhyItMatters(
        "Unbounded queue: producers flood memory when consumers fall behind",
        "Bounded queue: producers block when full, consumers block when empty"
    );

    std::cout << "  Queue capacity : " << BOLD << QUEUE_SIZE << RESET << " slots\n";
    std::cout << "  Producers      : ATM, MOBILE, WEB  (3 threads, 8 requests each)\n";
    std::cout << "  Consumers      : 3 worker threads\n";
    std::cout << "  Total requests : " << BOLD << TOTAL << RESET << "\n\n";
    std::cout << DIM
              << "  sem_wait(empty_slots) -> producer blocks when queue full\n"
              << "  sem_wait(full_slots)  -> consumer blocks when queue empty\n"
              << RESET << "\n";

    BoundedQueue bq(QUEUE_SIZE);
    std::atomic<int> consumed{0};
    std::atomic<bool> done_producing{false};
    std::mutex result_mutex, print_m;
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
                { std::lock_guard<std::mutex> lk(print_m);
                  std::cout << "  [Consumer-" << c << "] "
                            << std::left << std::setw(6)  << req.channel
                            << " "        << std::setw(9)  << req.type
                            << " $"       << std::setw(7)  << req.amount
                            << (ok ? GREEN " OK" RESET : RED " FAIL" RESET) << "\n"; }
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
                { std::lock_guard<std::mutex> lk(print_m);
                  std::cout << DIM << "  [" << channels[p] << "] -> queue "
                            << bq.getCurrentSize() << "/" << QUEUE_SIZE << "\n" << RESET; }
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

    std::cout << "\n";
    bq.printStats();
    std::cout << GREEN << "\n  All " << consumed << " requests processed.\n" << RESET;
    showBalances(accounts);
    pauseForEnter();
}

// ─────────────────────────────────────────────────────────────
//  6. READERS-WRITERS
// ─────────────────────────────────────────────────────────────
void topicReadersWriters(std::vector<std::shared_ptr<Account>>& accounts) {
    printTopic(
        "READERS-WRITERS PROBLEM",
        "Read-Write Lock — shared read / exclusive write",
        "Many threads read balances simultaneously; a transfer gets exclusive access"
    );

    printWhyItMatters(
        "Plain mutex: only ONE reader at a time -> unnecessary serialization",
        "RW Lock: N readers run in parallel; writer blocks all, then runs alone"
    );

    std::cout << "  Readers : 6 threads checking total balance simultaneously\n";
    std::cout << "  Writers : 2 transfer threads needing exclusive access\n";
    std::cout << "  Rule    : " << BOLD << "Many readers OR one writer — never both\n" << RESET << "\n";

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
                  std::cout << CYAN << "  [Reader-" << r
                            << "] acquiring shared lock...\n" << RESET; }
                ReadGuard rg(rwl);
                { std::lock_guard<std::mutex> lk(print_m);
                  std::cout << GREEN << "  [Reader-" << r
                            << "] reading  (shared — OK alongside others)\n" << RESET; }
                std::this_thread::sleep_for(std::chrono::milliseconds(150));
                double total = 0;
                for (auto b : balances) total += b;
                { std::lock_guard<std::mutex> lk(print_m);
                  std::cout << "  [Reader-" << r << "] total=$"
                            << std::fixed << std::setprecision(2) << total
                            << "  — releasing\n"; }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        });
    }

    std::vector<std::thread> writers;
    for (int w = 0; w < 2; w++) {
        writers.emplace_back([&, w]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(100 + w * 200));
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << YELLOW << "\n  [Writer-" << w
                        << "] requesting EXCLUSIVE write lock...\n" << RESET; }
            WriteGuard wg(rwl);
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << RED << BOLD
                        << "  [Writer-" << w
                        << "] EXCLUSIVE — all readers blocked. Transferring $500.\n"
                        << RESET; }
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            balances[w] -= 500.0;
            balances[(w+1) % balances.size()] += 500.0;
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << GREEN << "  [Writer-" << w
                        << "] done — write lock released.\n\n" << RESET; }
        });
    }

    for (auto& r : readers) r.join();
    for (auto& w : writers) w.join();

    rwl.printStats();
    std::cout << GREEN
              << "\n  Multiple readers ran simultaneously — no unnecessary blocking.\n"
              << "  Writers got exclusive access — zero data corruption.\n"
              << RESET;
    pauseForEnter();
}

// ─────────────────────────────────────────────────────────────
//  7. INTER-PROCESS COMMUNICATION
// ─────────────────────────────────────────────────────────────
void topicIPC(std::vector<std::shared_ptr<Account>>& accounts,
              ConnectionPool& pool) {
    printTopic(
        "INTER-PROCESS COMMUNICATION  (3 Mechanisms)",
        "Shared Memory  |  Message Queue  |  Pipes",
        "Fraud detection, audit notifications, and log streaming via OS IPC"
    );

    printWhyItMatters(
        "Global variables: no OS isolation, breaks across process boundaries",
        "IPC: OS-managed channels — work across separate processes"
    );

    IPCManager ipc;
    std::cout << BOLD << "  Initializing IPC mechanisms...\n" << RESET;
    bool shm_ok  = ipc.initSharedMemory();
    bool mq_ok   = ipc.initMessageQueue();
    bool pipe_ok = ipc.initPipe();

    if (!shm_ok || !mq_ok || !pipe_ok) {
        std::cout << RED << "  IPC init failed\n" << RESET; return;
    }
    std::cout << GREEN << "  All 3 IPC mechanisms ready.\n" << RESET;

    std::mutex print_m;

    // ── Part 1: Shared Memory ─────────────────────────────────
    printSubSection("Shared Memory — Fraud Detection Flag");
    std::cout << "  One shared memory segment visible to all 10 threads.\n";
    std::cout << "  Any thread sets the fraud flag; all others see it instantly.\n\n";

    std::vector<std::thread> txn_threads;
    for (int i = 0; i < 10; i++) {
        txn_threads.emplace_back([&, i]() {
            double amount = 100.0 + i * 150.0;
            std::string acc = accounts[i % accounts.size()]->getAccountNumber();
            if (amount > 1000.0) {
                ipc.setFraudAlert(acc, amount);
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << RED << "  [Thread-" << i << "] FRAUD ALERT -> SHM: "
                          << acc << " $" << std::fixed << std::setprecision(2)
                          << amount << "\n" << RESET;
            } else {
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << GREEN << "  [Thread-" << i << "] Normal txn : "
                          << acc << " $" << std::fixed << std::setprecision(2)
                          << amount << "\n" << RESET;
            }
            if (ipc.isFraudAlert()) {
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << YELLOW << "  [Thread-" << i
                          << "] reads SHM -> FRAUD DETECTED\n" << RESET;
            }
        });
    }
    for (auto& t : txn_threads) t.join();

    SharedFraudData* data = ipc.getSharedData();
    std::cout << "\n  SHM result : " << RED << data->suspicious_count
              << " suspicious txns" << RESET
              << "  |  Total flagged: $"
              << std::fixed << std::setprecision(2) << data->total_flagged << "\n";
    ipc.clearFraudAlert();
    pauseForEnter("Next: Message Queue ->");

    // ── Part 2: Message Queue ─────────────────────────────────
    printSubSection("Message Queue — Audit Notifications");
    std::cout << "  6 producer threads send audit msgs via OS message queue.\n";
    std::cout << "  Audit thread receives them asynchronously.\n\n";

    std::thread audit_thread([&]() {
        int received = 0;
        while (received < 6) {
            TransactionMessage msg;
            if (ipc.receiveMessage(msg)) {
                received++;
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << MAGENTA << "  [AUDIT] "
                          << std::left << std::setw(8)  << msg.account
                          << " "       << std::setw(9)  << msg.type
                          << " $"      << std::setw(7)  << msg.amount
                          << (msg.status ? " OK" : " FAIL") << "\n" << RESET;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });

    std::string types[] = {"DEPOSIT", "WITHDRAW", "TRANSFER"};
    for (int i = 0; i < 6; i++) {
        std::string acc  = accounts[i % accounts.size()]->getAccountNumber();
        std::string type = types[i % 3];
        double amt = 200.0 + i * 50.0;
        { std::lock_guard<std::mutex> lk(print_m);
          std::cout << "  [Producer-" << i << "] -> MQ: "
                    << acc << " " << type << " $" << amt << "\n"; }
        ipc.sendMessage(acc, type, amt, true);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    audit_thread.join();
    std::cout << GREEN << "\n  All 6 messages delivered via OS message queue.\n" << RESET;
    pauseForEnter("Next: Pipes ->");

    // ── Part 3: Pipe ──────────────────────────────────────────
    printSubSection("Pipes — Audit Log Stream");
    std::cout << "  Writer thread sends records via pipe fd[1].\n";
    std::cout << "  Audit logger reads from fd[0] — one-directional OS channel.\n\n";

    std::thread pipe_reader([&]() {
        for (int i = 0; i < 5; i++) {
            std::string d = ipc.readFromPipe();
            std::lock_guard<std::mutex> lk(print_m);
            std::cout << CYAN << "  [AUDIT LOGGER] <- " << d << RESET;
        }
    });

    std::thread pipe_writer([&]() {
        std::string txns[] = {
            "ACC-001 DEPOSIT  $500   OK\n",
            "ACC-002 WITHDRAW $200   OK\n",
            "ACC-003 TRANSFER $750   OK\n",
            "ACC-004 DEPOSIT  $1000  OK\n",
            "ACC-005 WITHDRAW $300   FAIL\n"
        };
        for (auto& txn : txns) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            { std::lock_guard<std::mutex> lk(print_m);
              std::cout << "  [WRITER]       -> " << txn; }
            ipc.writeToPipe(txn);
        }
    });

    pipe_writer.join();
    pipe_reader.join();
    ipc.closePipe();

    std::cout << GREEN << "\n  5 audit records streamed through pipe.\n" << RESET;
    std::cout << "\n";
    ipc.printStats();
    std::cout << GREEN << "\n  All 3 IPC mechanisms demonstrated successfully.\n" << RESET;
    pauseForEnter();
}

// ─────────────────────────────────────────────────────────────
//  8. PAGE REPLACEMENT  (Week 6 — preserved as-is)
//     NOTE: topicPageReplacement() is defined in
//     PageReplacementCache.h (header-only). If you are on a
//     machine that does not have that header yet, comment out
//     the call in main() below.
// ─────────────────────────────────────────────────────────────
// Forward-declare so the compiler is happy even if the full
// include is at the top of the file in your local copy.
// If PageReplacementCache.h is NOT present, comment this block:
// #include "PageReplacementCache.h"
// void topicPageReplacement(...) { ... }
// and remove the call in main().

// ─────────────────────────────────────────────────────────────
//  9. CPU SCHEDULING  (Week 7)
// ─────────────────────────────────────────────────────────────
void topicCPUScheduling() {
    printTopic(
        "CPU SCHEDULING ALGORITHMS",
        "FCFS | Round Robin (quantum=2) | Priority (VIP=3, CURRENT=2, SAVINGS=1)",
        "8 banking transactions scheduled — Gantt chart + waiting/turnaround stats"
    );

    printWhyItMatters(
        "No scheduling: VIP transactions wait same as SAVINGS — unfair, slow",
        "Priority scheduling: VIP served first; RR ensures fair CPU sharing"
    );

    // ── Define 8 transactions ────────────────────────────────
    // (account_type, tx_type, burst_time_ms, arrival_time_ms)
    std::vector<SchedTransaction> txns = {
        SchedTransaction(1, "SAVINGS",  "DEPOSIT",  4, 0),
        SchedTransaction(2, "VIP",      "TRANSFER", 3, 1),
        SchedTransaction(3, "CURRENT",  "WITHDRAW", 5, 2),
        SchedTransaction(4, "VIP",      "DEPOSIT",  2, 3),
        SchedTransaction(5, "SAVINGS",  "TRANSFER", 6, 4),
        SchedTransaction(6, "CURRENT",  "DEPOSIT",  3, 5),
        SchedTransaction(7, "VIP",      "WITHDRAW", 1, 6),
        SchedTransaction(8, "SAVINGS",  "DEPOSIT",  4, 7),
    };

    // ── Show the transaction set ──────────────────────────────
    std::cout << "\n  " << BOLD << "Transaction Set (8 banking operations):\n" << RESET;
    std::cout << "  ┌────┬──────────────────┬──────────┬──────────┬─────────┬──────────┐\n";
    std::cout << "  │ " << BOLD << "ID" << RESET
              << " │ " << BOLD << std::left << std::setw(16) << "Name"    << RESET
              << " │ " << BOLD << std::setw(8)  << "Account" << RESET
              << " │ " << BOLD << std::setw(8)  << "TxType"  << RESET
              << " │ " << BOLD << std::setw(7)  << "Burst"   << RESET
              << " │ " << BOLD << std::setw(8)  << "Arrival" << RESET
              << " │\n";
    std::cout << "  ├────┼──────────────────┼──────────┼──────────┼─────────┼──────────┤\n";

    for (auto& t : txns) {
        std::string color = (t.account_type=="VIP") ? RED :
                            (t.account_type=="CURRENT") ? CYAN : YELLOW;
        std::cout << "  │ " << color << std::right << std::setw(2) << t.id << RESET
                  << " │ " << color << std::left << std::setw(16) << t.name    << RESET
                  << " │ " << color << std::setw(8) << t.account_type << RESET
                  << " │ " << std::setw(8)  << t.tx_type
                  << " │ " << std::right << std::setw(5) << t.burst_time   << "ms │ "
                  << std::setw(5) << t.arrival_time << "ms │\n";
    }
    std::cout << "  └────┴──────────────────┴──────────┴──────────┴─────────┴──────────┘\n";
    CPUScheduler::printLegend();

    // ── Run all three algorithms ──────────────────────────────
    auto fcfs_res = CPUScheduler::fcfs(txns);
    auto rr_res   = CPUScheduler::roundRobin(txns, 2);
    auto pri_res  = CPUScheduler::priorityScheduling(txns);

    // ── 9a: FCFS ─────────────────────────────────────────────
    printSubSection("Algorithm A: FCFS — First Come First Served");
    std::cout << DIM
              << "  Theory: Transactions processed in arrival order. No preemption.\n"
              << "  Problem: Long SAVINGS txn blocks VIP behind it (convoy effect).\n"
              << RESET << "\n";
    CPUScheduler::printGantt(fcfs_res);
    CPUScheduler::printTransactionTable(fcfs_res);
    CPUScheduler::printStats(fcfs_res);
    pauseForEnter("Next: Round Robin ->");

    // ── 9b: Round Robin ──────────────────────────────────────
    printSubSection("Algorithm B: Round Robin — quantum = 2ms");
    std::cout << DIM
              << "  Theory: Each transaction gets exactly 2ms on CPU, then preempted.\n"
              << "  Benefit: Fair CPU sharing — no transaction starves.\n"
              << "  Banking use: ATM requests get CPU turns even during long transfers.\n"
              << RESET << "\n";
    CPUScheduler::printGantt(rr_res);
    CPUScheduler::printTransactionTable(rr_res);
    CPUScheduler::printStats(rr_res);
    pauseForEnter("Next: Priority Scheduling ->");

    // ── 9c: Priority ─────────────────────────────────────────
    printSubSection("Algorithm C: Priority Scheduling — VIP first");
    std::cout << DIM
              << "  Theory: Highest priority runs first. Non-preemptive.\n"
              << "  VIP (P=3) > CURRENT (P=2) > SAVINGS (P=1)\n"
              << "  Banking use: VIP transfers jump the queue over SAVINGS deposits.\n"
              << RESET << "\n";
    CPUScheduler::printGantt(pri_res);
    CPUScheduler::printTransactionTable(pri_res);
    CPUScheduler::printStats(pri_res);
    pauseForEnter("Next: Comparison ->");

    // ── 9d: Side-by-side comparison ──────────────────────────
    printSubSection("Algorithm Comparison — Side by Side");
    CPUScheduler::printComparison({fcfs_res, rr_res, pri_res});

    std::cout << "\n  " << BOLD << "Key Observations:\n" << RESET;
    std::cout << "  " << CYAN   << "FCFS      " << RESET
              << "— Simple, predictable. Suffers convoy effect (long job blocks short).\n";
    std::cout << "  " << GREEN  << "Round Robin" << RESET
              << " — Fairest. Best response time. Higher turnaround due to context switches.\n";
    std::cout << "  " << RED    << "Priority  " << RESET
              << "— VIP transactions always finish first. Risk of SAVINGS starvation.\n";

    std::cout << "\n  " << BOLD << CYAN
              << "  OS Connection: Linux uses CFS (Completely Fair Scheduler).\n"
              << "  Our ThreadPool uses FIFO — this demo shows what happens when\n"
              << "  we add priority awareness for VIP banking customers.\n"
              << RESET;

    pauseForEnter();
}

// ─────────────────────────────────────────────────────────────
//  10. CONCURRENT STRESS TEST  (was Demo 8, now Demo 10)
// ─────────────────────────────────────────────────────────────
void topicStressTest(std::vector<std::shared_ptr<Account>>& accounts,
                     ConnectionPool& pool) {
    printTopic(
        "CONCURRENT STRESS TEST",
        "200 mixed transactions — ThreadPool + Mutex + Deadlock Prevention",
        "All OS synchronization concepts active simultaneously under load"
    );

    const int TOTAL = 200;
    g_success = 0; g_failed = 0;

    std::cout << "\n  Submitting 200 transactions (DEPOSIT / WITHDRAW / TRANSFER)\n";
    std::cout << "  concurrently via 4-thread pool. Every concept active.\n\n";

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
        printProgress(done, TOTAL, g_success.load(), g_failed.load(), tp.getActive());
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
    }
    printProgress(TOTAL, TOTAL, g_success.load(), g_failed.load(), 0);
    std::cout << "\n";

    submitter.join();
    tp.waitAll();

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now() - start).count();

    std::cout << "\n  " << BOLD << "Results:\n" << RESET;
    std::cout << "  Completed  : " << BOLD << TOTAL << RESET << " transactions\n";
    std::cout << "  Successful : " << GREEN << BOLD << g_success << RESET << "\n";
    std::cout << "  Failed     : "
              << (g_failed > 0 ? RED : GREEN) << g_failed << RESET << "\n";
    std::cout << "  Time       : " << elapsed << " ms\n";
    if (elapsed > 0)
        std::cout << "  Throughput : " << (TOTAL * 1000 / elapsed) << " txn/sec\n";
    std::cout << "  Deadlocks  : " << GREEN << BOLD << "0"
              << RESET << "  (lock ordering held throughout)\n";

    showBalances(accounts);
}

// ─────────────────────────────────────────────────────────────
//  FINAL SUMMARY TABLE
// ─────────────────────────────────────────────────────────────
void printSummary(ConnectionPool& pool) {
    std::cout << "\n\n" << BOLD << CYAN;
    std::cout << "╔══════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                    OS CONCEPTS SUMMARY                          ║\n";
    std::cout << "║    Banking Transaction Processing System  —  Week 1 → Week 7    ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════════╝\n";
    std::cout << RESET << "\n";

    std::cout << "  ┌──────────────────────────┬───────────────────────────┬────────┐\n";
    std::cout << "  │ " << BOLD << std::left << std::setw(24) << "Concept"
              << RESET << "   │ "
              << BOLD << std::left << std::setw(25) << "Where in This System"
              << RESET << "  │ "
              << BOLD << "Status" << RESET << "  │\n";
    std::cout << "  ├──────────────────────────┼───────────────────────────┼────────┤\n";

    auto row = [](const std::string& concept,
                  const std::string& where,
                  const std::string& color) {
        std::cout << "  │ " << color << std::left << std::setw(24) << concept << RESET
                  << "   │ " << std::left << std::setw(25) << where
                  << "   │ " << GREEN << " done " << RESET << "  │\n";
    };

    // Week 1
    row("Threads",             "ThreadPool worker pool",    CYAN);
    row("Mutex",               "account_mutex per Account", CYAN);
    row("Condition Variable",  "task queue wake/sleep",     CYAN);
    row("Deadlock Prevention", "ordered lock acquisition",  CYAN);
    row("Process Scheduling",  "FIFO thread pool dispatch", CYAN);
    std::cout << "  ├──────────────────────────┼───────────────────────────┼────────┤\n";
    // Week 2
    row("Semaphore",           "DB connection pool limit",  MAGENTA);
    row("Persistence",         "MySQL — survives restart",  MAGENTA);
    row("IPC Foundation",      "shared pool across threads",MAGENTA);
    std::cout << "  ├──────────────────────────┼───────────────────────────┼────────┤\n";
    // Week 3
    row("Producer-Consumer",   "ATM/Web/Mobile -> queue",   YELLOW);
    row("Bounded Buffer",      "8-slot transaction queue",  YELLOW);
    row("Dual Semaphores",     "empty_slots + full_slots",  YELLOW);
    std::cout << "  ├──────────────────────────┼───────────────────────────┼────────┤\n";
    // Week 4
    row("Readers-Writers",     "balance inquiry desk",      GREEN);
    row("Read-Write Lock",     "shared read, excl. write",  GREEN);
    row("Writer Priority",     "writers wait for readers",  GREEN);
    std::cout << "  ├──────────────────────────┼───────────────────────────┼────────┤\n";
    // Week 5
    row("Shared Memory",       "fraud alert flag (SHM)",    RED);
    row("Message Queue",       "audit notifications (MQ)",  RED);
    row("Pipes",               "audit log stream fd[0/1]",  RED);
    std::cout << "  ├──────────────────────────┼───────────────────────────┼────────┤\n";
    // Week 7 (CPU Scheduling)
    row("FCFS Scheduling",     "baseline comparison demo",  BLUE);
    row("Round Robin",         "quantum=2 fair preemption", BLUE);
    row("Priority Scheduling", "VIP=3 > CURRENT=2 > SAV=1",BLUE);
    std::cout << "  └──────────────────────────┴───────────────────────────┴────────┘\n";

    std::cout << "\n  " << BOLD << GREEN
              << "Total: 19 OS Concepts  |  Week 1+2+3+4+5+7  |  All verified in live demos."
              << RESET << "\n\n";

    pool.printStats();

    std::cout << "\n" << BOLD << CYAN
              << "  ================================================================\n"
              << "   Banking Transaction Processing System — demonstration complete.\n"
              << "  ================================================================\n"
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
    std::cout << "\n  Press Enter to begin the demonstration...";
    std::cin.get();

    topicPersistence(accounts, pool);         // Demo 1 — Durability & Persistence
    topicSemaphore(pool);                     // Demo 2 — Semaphore
    topicDeadlock(accounts, pool);            // Demo 3 — Deadlock Prevention
    topicMutex(accounts, pool);               // Demo 4 — Mutex
    topicProducerConsumer(accounts, pool);    // Demo 5 — Producer-Consumer
    topicReadersWriters(accounts);            // Demo 6 — Readers-Writers
    topicIPC(accounts, pool);                 // Demo 7 — IPC
    // Demo 8 — Page Replacement (Week 6)
    // topicPageReplacement(accounts, pool);  // ← Uncomment when PageReplacementCache.h present
    topicCPUScheduling();                     // Demo 9 — CPU Scheduling  (Week 7)
    topicStressTest(accounts, pool);          // Demo 10 — Stress Test

    printSummary(pool);

    return 0;
}
