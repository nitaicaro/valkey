# Forkless save running on a replica, interacting with the replication stream.

start_server {tags {"repl external:skip"} overrides {
    save ""
    repl-diskless-sync yes
    repl-diskless-sync-delay 0
}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {overrides {
        forkless-infrastructure-enabled yes
        save ""
        repl-diskless-load swapdb
    }} {
        set replica [srv 0 client]

        test "full sync with repl-diskless-load swapdb aborts an in-flight forkless bgsave" {
            $primary set onlykey v1

            # Enough local keys that the iterator is still part way through the
            # keyspace when the swap happens.
            $replica debug populate 100000
            assert_equal 100000 [$replica dbsize]

            $replica config set rdb-key-save-delay 200
            $replica config set bgsave-default-method forkless
            $replica bgsave
            wait_for_condition 50 100 {
                [s rdb_bgsave_in_progress] == 1 &&
                [s rdb_current_bgsave_type] eq "forkless"
            } else {
                fail "forkless bgsave did not start"
            }

            set log_from [count_log_lines 0]
            $replica replicaof $primary_host $primary_port
            wait_for_log_messages 0 {"*Discarding old DB in background*"} $log_from 100 100

            # swapdb replaces the dataset without emptying it, so bgIteration has
            # to be told the kvstores it captured are going away. Before the fix
            # the iterator kept scanning the discarded kvstore: it either crashed
            # in fullScanIteratorGetEntries, or silently finished early and
            # reported a snapshot that holds a fraction of the keys.
            wait_for_log_messages 0 {"*forkless-save: completion proc - terminated*"} $log_from 100 100

            assert_equal {PONG} [$replica ping]

            wait_for_sync $replica
            assert_equal 1 [$replica dbsize]
            assert_equal {v1} [$replica get onlykey]

            $replica config set rdb-key-save-delay 0
            $replica replicaof no one
            set _ {}
        } {} {needs:debug}
    }
}
