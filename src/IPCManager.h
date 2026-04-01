#ifndef IPCMANAGER_H
#define IPCMANAGER_H

/*
 * IPCManager — Week 5
 *
 * Demonstrates 3 POSIX IPC mechanisms:
 *
 * ─────────────────────────────────────────────────────────────
 * 1. SHARED MEMORY (shmget/shmat/shmdt/shmctl)
 *    - OS allocates a memory segment shared between processes
 *    - Any process with the key can attach and read/write
 *    - Used here: fraud alert flag shared across all threads
 *    - Fastest IPC — no kernel involvement after setup
 *
 * 2. MESSAGE QUEUE (msgget/msgsnd/msgrcv)
 *    - OS maintains a queue of typed messages
 *    - Producer sends, consumer receives by message type
 *    - Used here: transaction notifications sent to audit thread
 *    - Persistent — messages survive even if sender exits
 *
 * 3. PIPES (pipe())
 *    - Unidirectional byte stream between two ends (fd[0], fd[1])
 *    - Writer writes to fd[1], reader reads from fd[0]
 *    - Used here: transaction results piped to audit logger
 *    - Simplest IPC — kernel buffer, blocking read/write
 * ─────────────────────────────────────────────────────────────
 */

#include <iostream>
#include <string>
#include <cstring>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include <chrono>
#include <sstream>

// ── Shared Memory Layout ─────────────────────────────────────
struct SharedFraudData {
    int   fraud_alert;        // 0 = safe, 1 = alert
    int   suspicious_count;   // Number of suspicious transactions
    char  last_alert[64];     // Last alert message
    double total_flagged;     // Total amount flagged
};

// ── Message Queue Structure ───────────────────────────────────
struct TransactionMessage {
    long msg_type;            // Message type (1=deposit, 2=withdraw, 3=transfer)
    char account[20];         // Account number
    char type[20];            // Transaction type string
    double amount;            // Transaction amount
    int status;               // 1=success, 0=failed
};

class IPCManager {
private:
    // Shared Memory
    int    shm_id;
    SharedFraudData* shm_ptr;
    key_t  shm_key;

    // Message Queue
    int    msg_id;
    key_t  msg_key;

    // Pipe
    int    pipe_fd[2];        // pipe_fd[0]=read, pipe_fd[1]=write

    // Stats
    int msgs_sent{0};
    int msgs_received{0};
    int pipe_writes{0};
    int pipe_reads{0};

public:
    IPCManager() : shm_id(-1), shm_ptr(nullptr), msg_id(-1) {
        shm_key = ftok("/tmp", 'B');  // Generate IPC key
        msg_key = ftok("/tmp", 'M');
    }

    // ── SHARED MEMORY ─────────────────────────────────────────
    bool initSharedMemory() {
        // OS: shmget — create/get shared memory segment
        shm_id = shmget(shm_key, sizeof(SharedFraudData),
                        IPC_CREAT | 0666);
        if (shm_id < 0) {
            std::cerr << "[SHM] Failed to create shared memory\n";
            return false;
        }

        // OS: shmat — attach process to shared memory segment
        shm_ptr = (SharedFraudData*)shmat(shm_id, nullptr, 0);
        if (shm_ptr == (void*)-1) {
            std::cerr << "[SHM] Failed to attach shared memory\n";
            return false;
        }

        // Initialize shared data
        memset(shm_ptr, 0, sizeof(SharedFraudData));
        shm_ptr->fraud_alert = 0;
        shm_ptr->suspicious_count = 0;
        shm_ptr->total_flagged = 0.0;
        strcpy(shm_ptr->last_alert, "No alerts");

        std::cout << "[SHM] Shared memory created (id=" << shm_id
                  << ", size=" << sizeof(SharedFraudData) << " bytes)\n";
        return true;
    }

    void setFraudAlert(const std::string& account, double amount) {
        if (!shm_ptr) return;
        shm_ptr->fraud_alert = 1;
        shm_ptr->suspicious_count++;
        shm_ptr->total_flagged += amount;
        snprintf(shm_ptr->last_alert, 64, "ALERT: %s $%.2f",
                 account.c_str(), amount);
    }

    void clearFraudAlert() {
        if (!shm_ptr) return;
        shm_ptr->fraud_alert = 0;
        strcpy(shm_ptr->last_alert, "Cleared");
    }

    bool isFraudAlert() {
        return shm_ptr && shm_ptr->fraud_alert == 1;
    }

    SharedFraudData* getSharedData() { return shm_ptr; }

    void cleanupSharedMemory() {
        if (shm_ptr) {
            shmdt(shm_ptr);   // OS: shmdt — detach
            shm_ptr = nullptr;
        }
        if (shm_id >= 0) {
            shmctl(shm_id, IPC_RMID, nullptr);  // OS: shmctl — remove
            shm_id = -1;
        }
    }

    // ── MESSAGE QUEUE ─────────────────────────────────────────
    bool initMessageQueue() {
        // OS: msgget — create message queue
        msg_id = msgget(msg_key, IPC_CREAT | 0666);
        if (msg_id < 0) {
            std::cerr << "[MQ] Failed to create message queue\n";
            return false;
        }
        std::cout << "[MQ] Message queue created (id=" << msg_id << ")\n";
        return true;
    }

    void sendMessage(const std::string& account,
                     const std::string& type,
                     double amount, bool success) {
        if (msg_id < 0) return;

        TransactionMessage msg;
        if (type == "DEPOSIT")  msg.msg_type = 1;
        else if (type == "WITHDRAW") msg.msg_type = 2;
        else msg.msg_type = 3;

        strncpy(msg.account, account.c_str(), 19);
        strncpy(msg.type, type.c_str(), 19);
        msg.amount = amount;
        msg.status = success ? 1 : 0;

        // OS: msgsnd — send message to queue
        if (msgsnd(msg_id, &msg, sizeof(msg) - sizeof(long), 0) == 0) {
            msgs_sent++;
        }
    }

    bool receiveMessage(TransactionMessage& msg, long type = 0) {
        if (msg_id < 0) return false;

        // OS: msgrcv — receive message from queue (non-blocking)
        ssize_t result = msgrcv(msg_id, &msg,
                                sizeof(msg) - sizeof(long),
                                type, IPC_NOWAIT);
        if (result > 0) {
            msgs_received++;
            return true;
        }
        return false;
    }

    void cleanupMessageQueue() {
        if (msg_id >= 0) {
            msgctl(msg_id, IPC_RMID, nullptr);  // OS: msgctl — remove queue
            msg_id = -1;
        }
    }

    // ── PIPE ──────────────────────────────────────────────────
    bool initPipe() {
        // OS: pipe() — create unidirectional pipe
        if (pipe(pipe_fd) < 0) {
            std::cerr << "[PIPE] Failed to create pipe\n";
            return false;
        }
        std::cout << "[PIPE] Pipe created (read_fd=" << pipe_fd[0]
                  << ", write_fd=" << pipe_fd[1] << ")\n";
        return true;
    }

    void writeToPipe(const std::string& data) {
        std::string msg = data + "\n";
        // OS: write() — write to pipe (blocks if buffer full)
        write(pipe_fd[1], msg.c_str(), msg.size());
        pipe_writes++;
    }

    std::string readFromPipe() {
        char buf[256] = {0};
        // OS: read() — read from pipe (blocks until data available)
        ssize_t n = read(pipe_fd[0], buf, sizeof(buf) - 1);
        if (n > 0) {
            pipe_reads++;
            return std::string(buf, n);
        }
        return "";
    }

    void closePipe() {
        close(pipe_fd[0]);
        close(pipe_fd[1]);
    }

    void printStats() const {
        std::cout << "\n[IPC STATS]\n";
        std::cout << "  Shared Memory  : " << sizeof(SharedFraudData)
                  << " bytes shared\n";
        std::cout << "  Messages sent  : " << msgs_sent << "\n";
        std::cout << "  Messages recvd : " << msgs_received << "\n";
        std::cout << "  Pipe writes    : " << pipe_writes << "\n";
        std::cout << "  Pipe reads     : " << pipe_reads << "\n";
    }

    ~IPCManager() {
        cleanupSharedMemory();
        cleanupMessageQueue();
    }
};

#endif
