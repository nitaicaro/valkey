# Forkless save running on a replica, interacting with the replication stream.

start_server {tags {"repl external:skip"} overrides {save ""}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {overrides {forkless-infrastructure-enabled yes save ""}} {
        set replica [srv 0 client]

        test "primary write to a key held by a replica's forkless bgsave does not block the primary link" {

            # Values above BGITER_MAX_CLONE_ITEM_BYTES, so the iterator cannot
            # clone them and holds the entries by reference instead.  A client
            # writing one of those keys has to wait for the iterator.
            set big [string repeat x 2000]
            for {set i 0} {$i < 2000} {incr i} {
                $primary set key$i $big
            }

            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            assert_equal 2000 [$replica dbsize]

            # Slow enough that the save is still holding keys while the writes
            # below are applied.
            $replica config set rdb-key-save-delay 1000
            $replica config set bgsave-default-method forkless
            $replica bgsave
            wait_for_condition 50 100 {
                [s rdb_bgsave_in_progress] == 1 &&
                [s rdb_current_bgsave_type] eq "forkless"
            } else {
                fail "forkless bgsave did not start"
            }

            set log_from [count_log_lines 0]

            # Overwrite the keys the iterator is working through.  The primary
            # link cannot be blocked, so it waits for the iterator instead.
            set big2 [string repeat y 2000]
            for {set i 0} {$i < 200} {incr i} {
                $primary set key$i $big2
            }

            # The replica must still be alive: before the fix the applying
            # client reached blockClient(c, BLOCKED_INUSE) and hit the
            # "replicated clients should never be blocked" assertion.
            assert_equal {PONG} [$replica ping]

            # Confirm the wait actually happened, so a change that stops the
            # iterator from holding keys turns into a visible test failure
            # rather than a silent pass.
            wait_for_log_messages 0 {"*synchronously waited*for in-use keys*"} $log_from 100 100

            # Replication must keep making progress across the wait.
            $primary set canary 1
            wait_for_ofs_sync $primary $replica
            assert_equal 1 [$replica get canary]

            $replica config set rdb-key-save-delay 0
            wait_for_condition 300 100 {
                [s rdb_bgsave_in_progress] == 0
            } else {
                fail "forkless bgsave did not finish"
            }
            assert_equal "ok" [s rdb_last_bgsave_status]

            $replica replicaof no one
            set _ {}
        } {} {needs:debug}
    }
}
