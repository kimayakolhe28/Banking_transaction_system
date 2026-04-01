// ─────────────────────────────────────────────────────────────
//  DEMO 8 — IPC: Shared Memory + Message Queue + Pipes (Week 5)
// ─────────────────────────────────────────────────────────────
void demoIPC(std::vector<std::shared_ptr<Account>>& accounts,
             ConnectionPool& pool) {
    printSection("DEMO 8: IPC - SHARED MEM + MSG QUEUE + PIPES  (Week 5)");

    std::cout << "\n  3 IPC mechanisms demonstrated:\n";
    std::cout << "  1. Shared Memory  — fraud alert flag visible to all threads\n";
    std::cout << "  2. Message Queue  — transaction notifications to audit thread\n";
    std::cout << "  3. Pipe           — results streamed to audit logger\n\n";

    IPCManager ipc;

    // Initialize all 3 IPC mechanisms
    std::cout << BOLD << "  [INIT] Setting up IPC mechanisms...\n" << RESET;
    bool shm_ok = ipc.initSharedMemory();
    bool mq_ok  = ipc.initMessageQueue();
    bool pipe_ok = ipc.initPipe();

    if (!shm_ok || !mq_ok || !pipe_ok) {
        std::cout << RED << "  IPC init failed\n" << RESET;
        return;
    }
    std::cout << GREEN << "  All 3 IPC mechanisms ready!\n\n" << RESET;

    std::mutex print_m;

    // ── Part 1: Shared Memory — Fraud Detection ───────────────
    std::cout << BOLD << CYAN << "  --- Part 1: SHARED MEMORY (Fraud Detection) ---\n" << RESET;
    std::cout << "  10 transaction threads share ONE fraud flag via OS shared memory\n\n";

    std::vector<std::thread> txn_threads;
    std::atomic<int> flagged{0};

    for (int i = 0; i < 10; i++) {
        txn_threads.emplace_back([&, i]() {
            double amount = 100.0 + i * 150.0;  // i=8,9 will be "suspicious"
            std::string acc = accounts[i % accounts.size()]->getAccountNumber();

            // Flag large transactions as suspicious
            if (amount > 1000.0) {
                ipc.setFraudAlert(acc, amount);  // Write to shared memory
                flagged++;
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << RED << "  [Thread-" << i << "] FRAUD ALERT written to SHM: "
                          << acc << " $" << amount << "\n" << RESET;
            } else {
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << GREEN << "  [Thread-" << i << "] Normal txn: "
                          << acc << " $" << amount << "\n" << RESET;
            }

            // All threads can READ the shared fraud flag
            if (ipc.isFraudAlert()) {
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << YELLOW << "  [Thread-" << i
                          << "] Reads SHM: FRAUD DETECTED — "
                          << ipc.getSharedData()->last_alert << "\n" << RESET;
            }
        });
    }
    for (auto& t : txn_threads) t.join();

    SharedFraudData* data = ipc.getSharedData();
    std::cout << "\n  [SHM RESULT] Suspicious transactions: "
              << data->suspicious_count
              << " | Total flagged: $" << data->total_flagged << "\n";
    ipc.clearFraudAlert();

    std::cout << "\n  Press Enter for Part 2...";
    std::cin.get();

    // ── Part 2: Message Queue — Audit Notifications ───────────
    std::cout << BOLD << CYAN << "\n  --- Part 2: MESSAGE QUEUE (Audit Notifications) ---\n" << RESET;
    std::cout << "  Transaction threads send msgs to queue\n";
    std::cout << "  Audit thread receives and logs them\n\n";

    std::atomic<bool> audit_done{false};

    // Audit consumer thread
    std::thread audit_thread([&]() {
        int received = 0;
        while (received < 6) {
            TransactionMessage msg;
            if (ipc.receiveMessage(msg)) {
                received++;
                std::lock_guard<std::mutex> lk(print_m);
                std::cout << MAGENTA << "  [AUDIT] Received: "
                          << msg.account << " " << msg.type
                          << " $" << msg.amount
                          << (msg.status ? " OK" : " FAIL") << "\n" << RESET;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        audit_done = true;
    });

    // Producer threads send messages
    std::string types[] = {"DEPOSIT", "WITHDRAW", "TRANSFER"};
    for (int i = 0; i < 6; i++) {
        std::string acc = accounts[i % accounts.size()]->getAccountNumber();
        std::string type = types[i % 3];
        double amt = 200.0 + i * 50.0;

        std::cout << "  [Producer-" << i << "] Sending to MQ: "
                  << acc << " " << type << " $" << amt << "\n";

        ipc.sendMessage(acc, type, amt, true);  // OS: msgsnd()
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    audit_thread.join();
    std::cout << GREEN << "\n  Message Queue: all 6 notifications delivered!\n" << RESET;

    std::cout << "\n  Press Enter for Part 3...";
    std::cin.get();

    // ── Part 3: Pipe — Audit Logger ───────────────────────────
    std::cout << BOLD << CYAN << "\n  --- Part 3: PIPES (Audit Logger) ---\n" << RESET;
    std::cout << "  Writer thread sends results through pipe\n";
    std::cout << "  Reader thread (audit logger) reads from other end\n\n";

    std::atomic<bool> pipe_done{false};

    // Reader thread (audit logger)
    std::thread pipe_reader([&]() {
        for (int i = 0; i < 5; i++) {
            std::string data = ipc.readFromPipe();  // OS: read() — blocks
            std::lock_guard<std::mutex> lk(print_m);
            std::cout << CYAN << "  [AUDIT LOGGER] Received via pipe: "
                      << data << RESET;
        }
        pipe_done = true;
    });

    // Writer thread
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
            std::lock_guard<std::mutex> lk(print_m);
            std::cout << "  [WRITER] Writing to pipe: " << txn << "\n";
            print_m.unlock();
            ipc.writeToPipe(txn);  // OS: write() to pipe fd[1]
            print_m.lock();
        }
    });

    pipe_writer.join();
    pipe_reader.join();
    ipc.closePipe();

    std::cout << GREEN << "\n  Pipe: all 5 audit records streamed!\n" << RESET;

    ipc.printStats();

    std::cout << GREEN << "\n  IPC Demo complete!\n" << RESET;
    std::cout << "  3 IPC mechanisms working together in one banking system\n";

    std::cout << "\n  Press Enter to continue...";
    std::cin.get();
}
