set ::singledb 1

foreach type {fork thread} {
    start_server {overrides {forkless-options-supported yes save ""}} {
        # Populate ~6GB: 1.5M keys x 4KB
        set port [srv 0 port]
        exec ./src/valkey-benchmark -p $port -n 1500000 -r 1500000 -d 4096 -t set --sequential -q > /dev/null 2>&1
        puts "$type: starting with used_memory=[expr {[s used_memory]/1048576}]MB, dbsize=[r dbsize]"

        test "Experiment 1: replica full sync memory - $type" {
            # Write load: 1 client, sequential, pipeline 16, overwrite all 1.5M keys with same size
            set bench_pid [exec ./src/valkey-benchmark -p $port -r 1500000 -n 1500000 -d 4096 -c 1 -P 16 -t set --sequential -q > /dev/null 2>&1 &]

            start_server {overrides {forkless-options-supported yes save ""}} {
                if {$type eq "thread"} {
                    r -1 config set threadsave-enabled-for-replication yes
                }
                r -1 config set client-output-buffer-limit "slave 0 0 0"
                r slaveof [srv -1 host] [srv -1 port]

                puts [format "$type: %8s %8s %8s %10s" time_s total_rss_mb cow_mb cob_mb]
                set start [clock milliseconds]
                set peak_rss 0
                set peak_cow 0
                set child_exited 0
                set bench_exited 0
                set primary_pid [s -1 process_id]
                while {1} {
                    set elapsed [expr {([clock milliseconds] - $start) / 1000.0}]
                    set total_rss [exec bash -c "ps -o rss= --ppid $primary_pid --pid $primary_pid 2>/dev/null | awk '{s+=\$1}END{print int(s/1024)}'"]
                    # Read CoW directly from child's smaps_rollup (Private_Dirty)
                    set cow 0
                    set child_pids [exec bash -c "pgrep -P $primary_pid 2>/dev/null || true"]
                    foreach cpid $child_pids {
                        catch {
                            set cow [exec bash -c "awk '/Private_Dirty/{s+=\$2}END{print int(s/1024)}' /proc/$cpid/smaps_rollup"]
                        }
                    }
                    if {$child_exited == 0 && $child_pids eq "" && $peak_cow > 0} {
                        set child_exited 1
                        puts "$type: >>> CHILD EXITED at t=[format %.1f $elapsed]s <<<"
                    }
                    if {$bench_exited == 0 && ![file exists /proc/$bench_pid]} {
                        set bench_exited 1
                        puts "$type: >>> BENCHMARK FINISHED at t=[format %.1f $elapsed]s <<<"
                    }
                    set cob [expr {[s -1 mem_clients_slaves]/1048576}]
                    if {$total_rss > $peak_rss} {set peak_rss $total_rss}
                    if {$cow > $peak_cow} {set peak_cow $cow}
                    puts [format "$type: %8.1f %8d %8d %10d" $elapsed $total_rss $cow $cob]

                    if {[string match *up* [r info replication]]} break
                    if {$elapsed > 120} {fail "sync took too long"; break}
                    after 100
                }
                # Keep measuring for 10 more seconds to show memory recovery
                set sync_done [clock milliseconds]
                while {[expr {([clock milliseconds] - $sync_done) / 1000.0}] < 10} {
                    set elapsed [expr {([clock milliseconds] - $start) / 1000.0}]
                    set total_rss [exec bash -c "ps -o rss= --ppid $primary_pid --pid $primary_pid 2>/dev/null | awk '{s+=\$1}END{print int(s/1024)}'"]
                    set cob [expr {[s -1 mem_clients_slaves]/1048576}]
                    puts [format "$type: %8.1f %8d %8d %10d" $elapsed $total_rss 0 $cob]
                    after 100
                }
                catch {exec kill $bench_pid}
                catch {exec pkill -f "valkey-benchmark.*-p $port"}
                set cow_final $peak_cow
                puts "$type: RESULTS — peak_total_rss=${peak_rss}MB, peak_cow=${cow_final}MB, time=[format %.1f $elapsed]s"
            }
        }
    }
}
