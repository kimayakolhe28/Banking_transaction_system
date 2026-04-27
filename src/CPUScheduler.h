#ifndef CPUSCHEDULER_H
#define CPUSCHEDULER_H

/*
 * CPUScheduler — Week 7
 *
 * OS Concepts implemented:
 * ─────────────────────────────────────────────────────────────
 * 1. ROUND ROBIN SCHEDULING
 *    - Each transaction gets a fixed time quantum (default=2ms)
 *    - Preempted transactions go to the back of the ready queue
 *    - Fair CPU sharing — no starvation
 *    - Real-time Gantt chart output showing execution slices
 *
 * 2. PRIORITY SCHEDULING (Non-Preemptive)
 *    - VIP accounts     → Priority 3 (highest)
 *    - CURRENT accounts → Priority 2
 *    - SAVINGS accounts → Priority 1 (lowest)
 *    - Higher priority transactions run first
 *    - Ties broken by arrival order (FCFS within same priority)
 *
 * 3. FCFS SCHEDULING (baseline comparison)
 *    - First Come First Served
 *    - Simple queue — no preemption, no priority
 *    - Used as baseline to show why better algorithms needed
 *
 * Stats computed:
 *    - Waiting Time      = start_time - arrival_time
 *    - Turnaround Time   = finish_time - arrival_time
 *    - Response Time     = first_run_time - arrival_time
 *    - CPU Utilization   = busy_time / total_time * 100
 *    - Throughput        = transactions / total_time
 * ─────────────────────────────────────────────────────────────
 */

#include <iostream>
#include <iomanip>
#include <vector>
#include <queue>
#include <string>
#include <algorithm>
#include <numeric>
#include <climits>

#include <sstream>

// ── Color macros (safe re-include guard) ─────────────────────
#ifndef RESET
#define RESET   "\033[0m"
#endif
#ifndef BOLD
#define BOLD    "\033[1m"
#endif
#ifndef DIM
#define DIM     "\033[2m"
#endif
#ifndef RED
#define RED     "\033[31m"
#endif
#ifndef GREEN
#define GREEN   "\033[32m"
#endif
#ifndef YELLOW
#define YELLOW  "\033[33m"
#endif
#ifndef CYAN
#define CYAN    "\033[36m"
#endif
#ifndef MAGENTA
#define MAGENTA "\033[35m"
#endif
#ifndef BLUE
#define BLUE    "\033[34m"
#endif

// ─────────────────────────────────────────────────────────────
//  Transaction (CPU Process equivalent)
// ─────────────────────────────────────────────────────────────
struct SchedTransaction {
    int         id;
    std::string name;           // e.g. "T1-VIP-DEPOSIT"
    std::string account_type;   // "VIP", "CURRENT", "SAVINGS"
    std::string tx_type;        // "DEPOSIT", "WITHDRAW", "TRANSFER"
    int         priority;       // 3=VIP, 2=CURRENT, 1=SAVINGS
    int         burst_time;     // CPU time needed (ms units)
    int         arrival_time;   // When it enters the system

    // Computed by scheduler
    int         remaining_time;
    int         start_time     = -1;   // first time on CPU
    int         finish_time    = 0;
    int         waiting_time   = 0;
    int         turnaround_time= 0;
    int         response_time  = -1;

    SchedTransaction(int id, const std::string& acct,
                     const std::string& tx, int burst, int arrival)
        : id(id), account_type(acct), tx_type(tx),
          burst_time(burst), arrival_time(arrival),
          remaining_time(burst) {
        // Assign priority from account type
        if      (acct == "VIP")     priority = 3;
        else if (acct == "CURRENT") priority = 2;
        else                        priority = 1;

        // Build readable name
        name = "T" + std::to_string(id) + "-" + acct.substr(0,3) + "-" + tx.substr(0,3);
    }
};

// ─────────────────────────────────────────────────────────────
//  Gantt Slot — one unit of execution on CPU
// ─────────────────────────────────────────────────────────────
struct GanttSlot {
    int         tx_id;
    std::string tx_name;
    std::string acct_type;
    int         time_start;
    int         time_end;
};

// ─────────────────────────────────────────────────────────────
//  Scheduler Results
// ─────────────────────────────────────────────────────────────
struct SchedulerResult {
    std::string              algorithm;
    std::vector<GanttSlot>   gantt;
    std::vector<SchedTransaction> completed;
    double avg_waiting_time;
    double avg_turnaround_time;
    double avg_response_time;
    double cpu_utilization;
    double throughput;
    int    total_time;
};

// ─────────────────────────────────────────────────────────────
//  CPUScheduler class
// ─────────────────────────────────────────────────────────────
class CPUScheduler {
public:

    // ── FCFS ─────────────────────────────────────────────────
    static SchedulerResult fcfs(std::vector<SchedTransaction> txns) {
        std::sort(txns.begin(), txns.end(),
                  [](const SchedTransaction& a, const SchedTransaction& b){
                      return a.arrival_time < b.arrival_time;
                  });

        SchedulerResult res;
        res.algorithm = "FCFS (First Come First Served)";
        int clock = 0;

        for (auto& t : txns) {
            if (clock < t.arrival_time) clock = t.arrival_time;

            if (t.start_time == -1) t.start_time = clock;
            t.response_time   = clock - t.arrival_time;
            t.finish_time     = clock + t.burst_time;
            t.waiting_time    = clock - t.arrival_time;
            t.turnaround_time = t.finish_time - t.arrival_time;

            res.gantt.push_back({t.id, t.name, t.account_type, clock, t.finish_time});
            clock = t.finish_time;
            res.completed.push_back(t);
        }

        computeStats(res);
        return res;
    }

    // ── ROUND ROBIN ──────────────────────────────────────────
    static SchedulerResult roundRobin(std::vector<SchedTransaction> txns,
                                       int quantum = 2) {
        SchedulerResult res;
        res.algorithm = "Round Robin (quantum=" + std::to_string(quantum) + ")";

        // Sort by arrival
        std::sort(txns.begin(), txns.end(),
                  [](const SchedTransaction& a, const SchedTransaction& b){
                      return a.arrival_time < b.arrival_time;
                  });

        std::queue<int> ready;   // indices into txns
        int clock = 0;
        int idx   = 0;           // next txn to add to ready queue
        int done  = 0;

        // Add all arrived at t=0
        while (idx < (int)txns.size() && txns[idx].arrival_time <= clock)
            ready.push(idx++);

        while (done < (int)txns.size()) {
            if (ready.empty()) {
                // CPU idle — advance to next arrival
                if (idx < (int)txns.size()) {
                    clock = txns[idx].arrival_time;
                    while (idx < (int)txns.size() && txns[idx].arrival_time <= clock)
                        ready.push(idx++);
                }
                continue;
            }

            int i = ready.front();
            ready.pop();

            // Record first response
            if (txns[i].start_time == -1) {
                txns[i].start_time   = clock;
                txns[i].response_time = clock - txns[i].arrival_time;
            }

            int run = std::min(quantum, txns[i].remaining_time);
            res.gantt.push_back({txns[i].id, txns[i].name,
                                  txns[i].account_type, clock, clock + run});
            clock                    += run;
            txns[i].remaining_time   -= run;

            // Add newly arrived transactions
            while (idx < (int)txns.size() && txns[idx].arrival_time <= clock)
                ready.push(idx++);

            if (txns[i].remaining_time > 0) {
                ready.push(i);   // back of queue
            } else {
                txns[i].finish_time     = clock;
                txns[i].waiting_time    = txns[i].finish_time
                                         - txns[i].arrival_time
                                         - txns[i].burst_time;
                txns[i].turnaround_time = txns[i].finish_time
                                         - txns[i].arrival_time;
                res.completed.push_back(txns[i]);
                done++;
            }
        }

        computeStats(res);
        return res;
    }

    // ── PRIORITY (Non-Preemptive) ────────────────────────────
    static SchedulerResult priorityScheduling(std::vector<SchedTransaction> txns) {
        SchedulerResult res;
        res.algorithm = "Priority Scheduling (VIP=3 > CURRENT=2 > SAVINGS=1)";

        int clock = 0;
        int done  = 0;
        std::vector<bool> completed(txns.size(), false);

        while (done < (int)txns.size()) {
            // Find highest-priority arrived transaction
            int best = -1;
            for (int i = 0; i < (int)txns.size(); i++) {
                if (completed[i]) continue;
                if (txns[i].arrival_time > clock) continue;
                if (best == -1 ||
                    txns[i].priority > txns[best].priority ||
                    (txns[i].priority == txns[best].priority &&
                     txns[i].arrival_time < txns[best].arrival_time)) {
                    best = i;
                }
            }

            if (best == -1) {
                // Nothing arrived yet — idle until next
                int next_arr = INT_MAX;
                for (int i = 0; i < (int)txns.size(); i++)
                    if (!completed[i])
                        next_arr = std::min(next_arr, txns[i].arrival_time);
                clock = next_arr;
                continue;
            }

            auto& t = txns[best];
            if (t.start_time == -1) t.start_time = clock;
            t.response_time   = clock - t.arrival_time;
            t.finish_time     = clock + t.burst_time;
            t.waiting_time    = clock - t.arrival_time;
            t.turnaround_time = t.finish_time - t.arrival_time;

            res.gantt.push_back({t.id, t.name, t.account_type, clock, t.finish_time});
            clock = t.finish_time;
            res.completed.push_back(t);
            completed[best] = true;
            done++;
        }

        computeStats(res);
        return res;
    }

    // ─────────────────────────────────────────────────────────
    //  DISPLAY HELPERS
    // ─────────────────────────────────────────────────────────

    // Gantt chart — terminal bar chart
    static void printGantt(const SchedulerResult& res) {
        std::cout << "\n  Gantt Chart:\n";
        std::cout << "  ";

        // Top border
        for (auto& s : res.gantt) {
            int width = std::max(2, s.time_end - s.time_start) * 3;
            std::cout << "┌" << std::string(width, '-') ;
        }
        std::cout << "┐\n  ";

        // Process names — colored by account type
        for (auto& s : res.gantt) {
            int width = std::max(2, s.time_end - s.time_start) * 3;
            std::string color = slotColor(s.acct_type);
            std::string label = s.tx_name;
            if ((int)label.size() > width) label = label.substr(0, width);
            int pad = width - (int)label.size();
            int lpad = pad / 2, rpad = pad - lpad;
            std::cout << "│" << color
                      << std::string(lpad, ' ') << label << std::string(rpad, ' ')
                      << RESET;
        }
        std::cout << "│\n  ";

        // Bottom border
        for (auto& s : res.gantt) {
            int width = std::max(2, s.time_end - s.time_start) * 3;
            std::cout << "└" << std::string(width, '-');
        }
        std::cout << "┘\n  ";

        // Time markers
        int prev = -1;
        for (auto& s : res.gantt) {
            int width = std::max(2, s.time_end - s.time_start) * 3;
            std::string ts = std::to_string(s.time_start);
            std::cout << ts;
            int used = (int)ts.size();
            if (prev == s.time_start) used = 0;  // don't double-print
            std::cout << std::string(std::max(0, width + 1 - used), ' ');
            prev = s.time_start;
        }
        // Last time
        if (!res.gantt.empty())
            std::cout << res.gantt.back().time_end;
        std::cout << "\n";
    }

    // Per-transaction breakdown table
    static void printTransactionTable(const SchedulerResult& res) {
        std::cout << "\n";
        std::cout << "  ┌──────┬──────────────────┬──────────┬─────────┬──────────┬─────────────┬────────────┐\n";
        std::cout << "  │ " << BOLD << "ID  " << RESET
                  << " │ " << BOLD << std::left << std::setw(16) << "Name" << RESET
                  << " │ " << BOLD << std::setw(8)  << "Burst"   << RESET
                  << " │ " << BOLD << std::setw(7)  << "Arrival" << RESET
                  << " │ " << BOLD << std::setw(8)  << "Finish"  << RESET
                  << " │ " << BOLD << std::setw(11) << "Wait"    << RESET
                  << " │ " << BOLD << std::setw(10) << "Turnaround" << RESET
                  << " │\n";
        std::cout << "  ├──────┼──────────────────┼──────────┼─────────┼──────────┼─────────────┼────────────┤\n";

        for (auto& t : res.completed) {
            std::string color = acctColor(t.account_type);
            std::cout << "  │ "
                      << color << std::setw(4) << t.id << RESET << " │ "
                      << color << std::left << std::setw(16) << t.name << RESET << " │ "
                      << std::right << std::setw(6) << t.burst_time << "ms │ "
                      << std::setw(5) << t.arrival_time << "ms │ "
                      << std::setw(6) << t.finish_time << "ms │ "
                      << (t.waiting_time>0?YELLOW:GREEN)
                      << std::setw(9) << t.waiting_time << "ms" << RESET << " │ "
                      << std::setw(8) << t.turnaround_time << "ms │\n";
        }

        std::cout << "  └──────┴──────────────────┴──────────┴─────────┴──────────┴─────────────┴────────────┘\n";
    }

    // Summary stats box
    static void printStats(const SchedulerResult& res) {
        std::cout << "\n";
        std::cout << "  ┌─ STATS ──────────────────────────────────────────────────┐\n";
        std::cout << "  │ Algorithm       : " << BOLD << CYAN
                  << std::left << std::setw(43) << res.algorithm << RESET << "│\n";
        std::cout << "  │ Avg Wait Time   : " << YELLOW
                  << std::left << std::setw(10) << (std::to_string((int)res.avg_waiting_time) + "ms")
                  << RESET << "  "
                  << DIM << "(lower = better scheduling)"
                  << std::string(14, ' ') << RESET << "│\n";
        std::cout << "  │ Avg Turnaround  : " << YELLOW
                  << std::left << std::setw(10) << (std::to_string((int)res.avg_turnaround_time) + "ms")
                  << RESET << "  "
                  << DIM << "(finish_time - arrival_time)"
                  << std::string(13, ' ') << RESET << "│\n";
        std::cout << "  │ Avg Response    : " << GREEN
                  << std::left << std::setw(10) << (std::to_string((int)res.avg_response_time) + "ms")
                  << RESET << "  "
                  << DIM << "(first_run - arrival_time)"
                  << std::string(14, ' ') << RESET << "│\n";
        std::cout << "  │ CPU Utilization : " << GREEN
                  << std::left << std::setw(10) << (std::to_string((int)res.cpu_utilization) + "%")
                  << RESET << "  "
                  << DIM << "(busy / total time)"
                  << std::string(20, ' ') << RESET << "│\n";
        std::cout << "  │ Throughput      : " << CYAN
                  << std::left << std::setw(10) << (std::to_string((int)(res.throughput*10)/10.0) + " tx/ms")
                  << RESET << "  "
                  << DIM << "(transactions per ms)"
                  << std::string(18, ' ') << RESET << "│\n";
        std::cout << "  └──────────────────────────────────────────────────────────┘\n";
    }

    // Comparison table — side by side all algorithms
    static void printComparison(const std::vector<SchedulerResult>& results) {
        std::cout << "\n  ┌─ ALGORITHM COMPARISON ──────────────────────────────────────────────┐\n";
        std::cout << "  │ " << BOLD
                  << std::left << std::setw(38) << "Algorithm"
                  << std::setw(12) << "Avg Wait"
                  << std::setw(15) << "Avg Turnaround"
                  << std::setw(12) << "CPU Util"
                  << RESET << "│\n";
        std::cout << "  ├────────────────────────────────────────────────────────────────────────┤\n";

        for (auto& r : results) {
            std::cout << "  │ "
                      << std::left << std::setw(38) << r.algorithm
                      << YELLOW << std::setw(12) << (std::to_string((int)r.avg_waiting_time) + "ms") << RESET
                      << GREEN  << std::setw(15) << (std::to_string((int)r.avg_turnaround_time) + "ms") << RESET
                      << CYAN   << std::setw(12) << (std::to_string((int)r.cpu_utilization) + "%") << RESET
                      << "│\n";
        }
        std::cout << "  └────────────────────────────────────────────────────────────────────────┘\n";
    }

    // Legend for account type colors
    static void printLegend() {
        std::cout << "\n  Legend: "
                  << RED    << "■ VIP (P=3)  " << RESET
                  << CYAN   << "■ CURRENT (P=2)  " << RESET
                  << YELLOW << "■ SAVINGS (P=1)  " << RESET
                  << "\n";
    }

private:
    // ── Internal helpers ──────────────────────────────────────
    static void computeStats(SchedulerResult& res) {
        if (res.completed.empty()) return;

        double sum_wait = 0, sum_turn = 0, sum_resp = 0;
        int busy = 0;
        for (auto& t : res.completed) {
            sum_wait += t.waiting_time;
            sum_turn += t.turnaround_time;
            sum_resp += (t.response_time >= 0 ? t.response_time : t.waiting_time);
        }

        // Total time from first arrival to last finish
        int first_arrival = res.completed[0].arrival_time;
        int last_finish   = 0;
        for (auto& t : res.completed) {
            first_arrival = std::min(first_arrival, t.arrival_time);
            last_finish   = std::max(last_finish,   t.finish_time);
        }
        for (auto& t : res.completed) busy += t.burst_time;

        int total = last_finish - first_arrival;
        res.total_time          = total;
        res.avg_waiting_time    = sum_wait / res.completed.size();
        res.avg_turnaround_time = sum_turn / res.completed.size();
        res.avg_response_time   = sum_resp / res.completed.size();
        res.cpu_utilization     = total > 0 ? (busy * 100.0 / total) : 0;
        res.throughput          = total > 0 ? (res.completed.size() * 1.0 / total) : 0;
    }

    static std::string slotColor(const std::string& acct_type) {
        if (acct_type == "VIP")     return RED;
        if (acct_type == "CURRENT") return CYAN;
        return YELLOW;
    }

    static std::string acctColor(const std::string& acct_type) {
        if (acct_type == "VIP")     return RED;
        if (acct_type == "CURRENT") return CYAN;
        return YELLOW;
    }
};

#endif // CPUSCHEDULER_H
