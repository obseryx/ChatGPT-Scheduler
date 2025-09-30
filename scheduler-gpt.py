#!/usr/bin/env python3
"""
scheduler-gpt.py

Team: (list team members here)
Implements: FIFO (FCFS), Preemptive SJF, Round Robin schedulers.

Usage:
    python scheduler-gpt.py inputFile.in

Output:
    Writes to inputFile.out

Specified error messages and output formatting follow the assignment instructions.
"""

import sys
import os
from collections import deque
from copy import deepcopy

# -----------------------
# Data structures
# -----------------------
class Process:
    def __init__(self, name, arrival, burst):
        self.name = name
        self.arrival = int(arrival)
        self.burst = int(burst)
        self.remaining = int(burst)
        self.start_time = None    # first time scheduled
        self.finish_time = None
        self.last_start = None    # when it last started executing (for RR accounting if needed)
        # statuses: Not arrived, Ready, Running, Finished
        self.state = "NotArrived"

    def is_finished(self):
        return self.remaining <= 0

    def turnaround(self):
        if self.finish_time is None:
            return None
        return self.finish_time - self.arrival

    def wait_time(self):
        t = self.turnaround()
        if t is None:
            return None
        return t - self.burst

    def response_time(self):
        if self.start_time is None:
            return None
        return self.start_time - self.arrival

    def __repr__(self):
        return f"Process({self.name},arr={self.arrival},burst={self.burst},rem={self.remaining})"


# -----------------------
# Parsing input
# -----------------------
def error(msg):
    print(msg)
    sys.exit(1)

def parse_input(filename):
    # Returns dict with keys: processcount, runfor, use, quantum (optional), processes:list
    if not os.path.isfile(filename):
        error(f"Error: input file '{filename}' not found.")
    params = {
        "processcount": None,
        "runfor": None,
        "use": None,
        "quantum": None,
        "processes": []
    }
    with open(filename, 'r') as f:
        for raw in f:
            # remove inline comments that start with '#', then strip whitespace
            # e.g. "process ... # comment" -> "process ..."
            line = raw.split('#', 1)[0].strip()
            if not line:
                continue
            parts = line.split()
            if parts[0].lower() == "processcount":
                if len(parts) < 2:
                    error("Error: Missing parameter processcount")
                params["processcount"] = int(parts[1])
            elif parts[0].lower() == "runfor":
                if len(parts) < 2:
                    error("Error: Missing parameter runfor")
                params["runfor"] = int(parts[1])
            elif parts[0].lower() == "use":
                if len(parts) < 2:
                    error("Error: Missing parameter use")
                params["use"] = parts[1].lower()
            elif parts[0].lower() == "quantum":
                if len(parts) < 2:
                    error("Error: Missing parameter quantum")
                params["quantum"] = int(parts[1])
            elif parts[0].lower() == "process":
                # expect: process name A arrival 0 burst 5
                # We'll be flexible with ordering but assume exactly this format as spec.
                try:
                    # naive parsing to support the shown format
                    # collect key-value pairs after 'process'
                    kv = {}
                    i = 1
                    while i < len(parts):
                        key = parts[i].lower()
                        if key == "name":
                            kv["name"] = parts[i+1]
                            i += 2
                        elif key == "arrival":
                            kv["arrival"] = parts[i+1]
                            i += 2
                        elif key == "burst":
                            kv["burst"] = parts[i+1]
                            i += 2
                        else:
                            # unknown token, skip it
                            i += 1
                    if "name" not in kv:
                        error("Error: Missing parameter name")
                    if "arrival" not in kv:
                        error("Error: Missing parameter arrival")
                    if "burst" not in kv:
                        error("Error: Missing parameter burst")
                    proc = Process(kv["name"], kv["arrival"], kv["burst"])
                    params["processes"].append(proc)
                except IndexError:
                    error("Error: Malformed process line")
            elif parts[0].lower() == "end":
                break
            else:
                # ignore unknown directives for now
                pass

    # basic validation
    if params["processcount"] is None:
        error("Error: Missing parameter processcount")
    if params["runfor"] is None:
        error("Error: Missing parameter runfor")
    if params["use"] is None:
        error("Error: Missing parameter use")
    if params["use"] == "rr" and params["quantum"] is None:
        error("Error: Missing quantum parameter when use is 'rr'")

    # If processcount doesn't match actual count, still proceed but warn (no spec requirement)
    return params

# -----------------------
# Helpers for output text
# -----------------------
def alg_display_name(use):
    if use in ("fcfs", "fifo"):
        return "Using First In First Out"
    elif use == "sjf":
        return "Using preemptive Shortest Job First"
    elif use == "rr":
        return "Using Round Robin"
    else:
        return f"Using {use}"

# -----------------------
# Scheduler implementations
# Each scheduler receives:
#   procs: list of Process objects (fresh copies)
#   runfor: number of ticks
#   quantum: for RR only (int or None)
# Returns:
#   events: list of strings for the per-tick events (in chronological order)
#   final_procs: list of processes with their times filled as possible
# -----------------------

def schedule_fcfs(procs, runfor):
    # Non-preemptive. When CPU becomes free, select next arrived process in arrival order that is ready.
    # Tie-break by arrival then by original order (we'll keep list order to reflect that).
    time = 0
    events = []
    procs = deepcopy(procs)
    n = len(procs)
    # sort procs by arrival preserving input order for ties
    procs_sorted = sorted(procs, key=lambda p: (p.arrival))
    ready_queue = deque()
    current = None

    # We'll maintain an index to add to ready queue when arrivals happen
    arrival_index = 0
    while time < runfor:
        # arrivals at this tick
        arrivals_this_tick = []
        while arrival_index < n and procs_sorted[arrival_index].arrival == time:
            p = procs_sorted[arrival_index]
            p.state = "Ready"
            ready_queue.append(p)
            arrivals_this_tick.append(p)
            arrival_index += 1
        for p in arrivals_this_tick:
            events.append(f"Time {time:3d} : {p.name} arrived")

        # if no current process, pick next from ready_queue
        if current is None or current.is_finished():
            if current is not None and current.is_finished() and current.finish_time is None:
                current.finish_time = time
                current.state = "Finished"
                events.append(f"Time {time:3d} : {current.name} finished")
                current = None

            if current is None:
                if ready_queue:
                    current = ready_queue.popleft()
                    if current.start_time is None:
                        current.start_time = time
                    current.state = "Running"
                    events.append(f"Time {time:3d} : {current.name} selected (burst {current.remaining:3d})")
        # run current process for one tick (if exists)
        if current is not None:
            current.remaining -= 1
            if current.is_finished():
                # finish event recorded at next loop iteration (or immediately)
                current.finish_time = time + 1
                current.state = "Finished"
                events.append(f"Time {time+1:3d} : {current.name} finished")
                current = None
        else:
            events.append(f"Time {time:3d} : Idle")
        time += 1

    # At end, mark any finished times that might not have been set (shouldn't happen)
    # Consolidate final process list by original names order
    return events, procs_sorted

def schedule_sjf_preemptive(procs, runfor):
    # Preemptive SJF (Shortest Remaining Time First)
    time = 0
    events = []
    procs = deepcopy(procs)
    n = len(procs)
    # For arrivals scanning
    procs_sorted = sorted(procs, key=lambda p: (p.arrival))
    arrival_index = 0
    current = None
    # ready list holds processes that have arrived and are not finished
    ready = []

    while time < runfor:
        # handle arrivals
        arrivals_this_tick = []
        while arrival_index < n and procs_sorted[arrival_index].arrival == time:
            p = procs_sorted[arrival_index]
            p.state = "Ready"
            ready.append(p)
            arrivals_this_tick.append(p)
            arrival_index += 1
        for p in arrivals_this_tick:
            events.append(f"Time {time:3d} : {p.name} arrived")

        # choose process with smallest remaining time among ready + current (if running)
        candidates = [p for p in ready if not p.is_finished()]
        if current is not None and not current.is_finished():
            candidates.append(current)

        if candidates:
            # choose by remaining time, tie-breaker arrival then name
            chosen = min(candidates, key=lambda p: (p.remaining, p.arrival, p.name))
            # if chosen is different from current, preempt/switch
            if chosen is not current:
                # record current's state (if any)
                if current is not None and not current.is_finished():
                    current.state = "Ready"
                    # ensure it is present in ready list
                    if current not in ready:
                        ready.append(current)
                # remove chosen from ready (if it was there)
                if chosen in ready:
                    ready.remove(chosen)
                current = chosen
                if current.start_time is None:
                    current.start_time = time
                current.state = "Running"
                events.append(f"Time {time:3d} : {current.name} selected (burst {current.remaining:3d})")
        else:
            # no candidate => idle
            if current is not None and current.is_finished():
                current = None
            events.append(f"Time {time:3d} : Idle")
            time += 1
            continue

        # execute current for 1 tick
        current.remaining -= 1
        # if finishes now
        if current.is_finished():
            current.finish_time = time + 1
            current.state = "Finished"
            events.append(f"Time {time+1:3d} : {current.name} finished")
            current = None

        time += 1

    # return final list of processes (we will output based on the original processes ordering later)
    return events, procs_sorted

def schedule_rr(procs, runfor, quantum):
    # Round Robin using quantum (preemptive after quantum or finish)
    time = 0
    events = []
    procs = deepcopy(procs)
    n = len(procs)
    procs_sorted = sorted(procs, key=lambda p: p.arrival)
    arrival_index = 0
    ready = deque()
    current = None
    current_quantum_used = 0  # how many ticks used in current quantum

    while time < runfor:
        # arrivals
        arrivals_this_tick = []
        while arrival_index < n and procs_sorted[arrival_index].arrival == time:
            p = procs_sorted[arrival_index]
            p.state = "Ready"
            ready.append(p)
            arrivals_this_tick.append(p)
            arrival_index += 1
        for p in arrivals_this_tick:
            events.append(f"Time {time:3d} : {p.name} arrived")

        # if no current, take next from ready
        if current is None or current.is_finished() or current_quantum_used >= quantum:
            # if current finished, record finish event (we might record immediate finish when it happened)
            if current is not None and current.is_finished():
                if current.finish_time is None:
                    current.finish_time = time
                current.state = "Finished"
                events.append(f"Time {time:3d} : {current.name} finished")
                current = None
                current_quantum_used = 0

            # if current used full quantum and still has remaining, preempt and requeue it
            if current is not None and not current.is_finished() and current_quantum_used >= quantum:
                # preempt and append to ready queue's tail
                current.state = "Ready"
                ready.append(current)
                current = None
                current_quantum_used = 0

            # pick next ready if available
            if current is None:
                if ready:
                    current = ready.popleft()
                    if current.start_time is None:
                        current.start_time = time
                    current.state = "Running"
                    current_quantum_used = 0
                    events.append(f"Time {time:3d} : {current.name} selected (burst {current.remaining:3d})")

        # run current for one tick if exists
        if current is not None:
            current.remaining -= 1
            current_quantum_used += 1
            if current.is_finished():
                current.finish_time = time + 1
                current.state = "Finished"
                events.append(f"Time {time+1:3d} : {current.name} finished")
                # current will be set to None next loop iteration
                current = None
                current_quantum_used = 0
        else:
            events.append(f"Time {time:3d} : Idle")
        time += 1

    return events, procs_sorted

# -----------------------
# Orchestration + Output
# -----------------------

def run_scheduler(params, infile):
    use = params["use"]
    runfor = params["runfor"]
    quantum = params.get("quantum", None)
    procs_input = params["processes"]

    # keep original order as given in input for final metrics listing
    original_order = deepcopy(procs_input)

    # pick scheduler
    if use in ("fcfs", "fifo"):
        events, final_procs_ref = schedule_fcfs(procs_input, runfor)
    elif use == "sjf":
        events, final_procs_ref = schedule_sjf_preemptive(procs_input, runfor)
    elif use == "rr":
        events, final_procs_ref = schedule_rr(procs_input, runfor, quantum)
    else:
        error(f"Error: Unknown scheduling algorithm '{use}'")

    # final timestamp printed
    finished_time_line = f"Finished at time {runfor}"

    # Build output lines
    output_lines = []
    output_lines.append(f"{len(procs_input)} processes")
    output_lines.append(alg_display_name(use))
    if use == "rr":
        output_lines.append(f"Quantum {quantum}")
    output_lines.append("")  # blank line for readability

    # Add events
    output_lines.extend(events)
    # If there were fewer events than runfor ticks, ensure Idle prints for missing ticks are present.
    # (Our schedulers already print per tick, so we won't add extras.)

    output_lines.append("")  # blank line
    output_lines.append(f"{finished_time_line}")
    output_lines.append("")  # blank line

    # Final metrics lines: for each original process in original input order
    # We must map to the processes inside final_procs_ref by name (they have same names)
    # final_procs_ref is sorted list (by arrival) inside the scheduler; get mapping
    proc_map = {p.name: p for p in final_procs_ref}
    for op in original_order:
        name = op.name
        if name not in proc_map:
            # shouldn't happen
            line = f"{name} did not finish"
            output_lines.append(line)
            continue
        p = proc_map[name]
        if p.finish_time is None:
            # Did not finish
            output_lines.append(f"{p.name} did not finish")
        else:
            wait = p.wait_time()
            turnaround = p.turnaround()
            response = p.response_time()
            # If any metric None, set to 0
            wait_s = str(wait if wait is not None else 0)
            ta_s = str(turnaround if turnaround is not None else 0)
            resp_s = str(response if response is not None else 0)
            output_lines.append(f"{p.name} wait {wait_s} turnaround {ta_s} response {resp_s}")

    return output_lines

# -----------------------
# Main entry
# -----------------------
def main():
    if len(sys.argv) != 2:
        print("Usage: scheduler-gpt.py <input file>")
        sys.exit(1)
    infile = sys.argv[1]
    # check extension .in
    # (spec says must be .in but doesn't instruct error. We'll proceed regardless.)
    params = parse_input(infile)
    out_lines = run_scheduler(params, infile)

    # write to output file with same base name .out
    base = os.path.splitext(infile)[0]
    outfile = base + ".out"
    with open(outfile, 'w') as f:
        for ln in out_lines:
            f.write(ln + "\n")

    # Also print to stdout for convenience
    for ln in out_lines:
        print(ln)

if __name__ == "__main__":
    main()
