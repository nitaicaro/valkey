tags {"rdb external:skip"} {

# Helper function to start a server and kill it, just to check the error
# logged.
set defaults {}
proc start_server_and_kill_it {overrides code} {
    upvar defaults defaults srv srv server_path server_path
    set config [concat $defaults $overrides]
    set srv [start_server [list overrides $config keep_persistence true]]
    uplevel 1 $code
    kill_server $srv
}

set server_path [tmpdir "server.rdb-encoding-test"]

# Copy RDB with different encodings in server path
exec cp tests/assets/encodings.rdb $server_path
exec cp tests/assets/encodings-rdb12.rdb $server_path
exec cp tests/assets/encodings-rdb75-unknown-types.rdb $server_path
exec cp tests/assets/encodings-rdb987.rdb $server_path
exec cp tests/assets/encodings-rdb987-unknown-types.rdb $server_path
exec cp tests/assets/list-quicklist.rdb $server_path

start_server [list overrides [list "dir" $server_path "dbfilename" "list-quicklist.rdb" save ""]] {
    test "test old version rdb file" {
        r select 0
        assert_equal [r get x] 7
        assert_encoding listpack list
        r lpop list
    } {7}
}

set csv_dump {"0","compressible","string","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
"0","hash","hash","a","1","aa","10","aaa","100","b","2","bb","20","bbb","200","c","3","cc","30","ccc","300","ddd","400","eee","5000000000",
"0","hash_zipped","hash","a","1","b","2","c","3",
"0","list","list","1","2","3","a","b","c","100000","6000000000","1","2","3","a","b","c","100000","6000000000","1","2","3","a","b","c","100000","6000000000",
"0","list_zipped","list","1","2","3","a","b","c","100000","6000000000",
"0","number","string","10"
"0","set","set","1","100000","2","3","6000000000","a","b","c",
"0","set_zipped_1","set","1","2","3","4",
"0","set_zipped_2","set","100000","200000","300000","400000",
"0","set_zipped_3","set","1000000000","2000000000","3000000000","4000000000","5000000000","6000000000",
"0","string","string","Hello World"
"0","zset","zset","a","1","b","2","c","3","aa","10","bb","20","cc","30","aaa","100","bbb","200","ccc","300","aaaa","1000","cccc","123456789","bbbb","5000000000",
"0","zset_zipped","zset","a","1","b","2","c","3",
}

start_server [list overrides [list "dir" $server_path "dbfilename" "encodings.rdb"]] {
  test "RDB encoding loading test" {
    r select 0
    csvdump r
  } $csv_dump
}

start_server_and_kill_it [list "dir" $server_path "dbfilename" "encodings-rdb987.rdb"] {
    test "RDB future version loading, strict version check" {
        wait_for_condition 50 100 {
            [string match {*Fatal error loading*} \
                 [exec tail -1 < [dict get $srv stdout]]]
        } else {
            fail "Server started even though RDB version is unsupported"
        }
    }
}

start_server [list overrides [list "dir" $server_path \
                                  "dbfilename" "encodings-rdb987.rdb" \
                                  "rdb-version-check" "relaxed"]] {
    test "RDB future version loading, relaxed version check" {
        r select 0
        csvdump r
    } $csv_dump
}

start_server_and_kill_it [list dir $server_path \
                              dbfilename "encodings-rdb987-unknown-types.rdb" \
                              rdb-version-check relaxed] {
    test "RDB future version loading with unknown types, relaxed version check" {
        wait_for_condition 50 100 {
            [string match {*Unknown type or opcode when loading DB. Unrecoverable error, aborting now.*} \
                 [exec tail -2 < [dict get $srv stdout]]]
        } else {
            fail "Server started even though RDB contains unknown types"
        }
    }
}

start_server [list overrides [list dir $server_path \
                                  dbfilename "encodings-rdb12.rdb" \
                                  rdb-version-check relaxed]] {
    test "RDB foreign version loading, relaxed version check" {
        r select 0
        assert_equal foo [r keys *]
        assert_equal bar [r get foo]
    }
}

start_server_and_kill_it [list dir $server_path \
                              dbfilename "encodings-rdb75-unknown-types.rdb" \
                              rdb-version-check relaxed] {
    test "RDB foreign version loading with unknown types, relaxed version check" {
        wait_for_condition 50 100 {
            [string match {*Can't handle foreign type or opcode 150 in RDB with version 75*} \
                 [exec tail -2 < [dict get $srv stdout]]]
        } else {
            fail "Server started even though RDB contains unknown types"
        }
    }
}

set server_path [tmpdir "server.rdb-startup-test"]

start_server [list overrides [list "dir" $server_path] keep_persistence true] {
    test {Server started empty with non-existing RDB file} {
        debug_digest
    } {0000000000000000000000000000000000000000}
    # Save an RDB file, needed for the next test.
    r save
}

start_server [list overrides [list "dir" $server_path] keep_persistence true] {
    test {Server started empty with empty RDB file} {
        debug_digest
    } {0000000000000000000000000000000000000000}
}

start_server [list overrides [list "dir" $server_path] keep_persistence true] {
    test {Test RDB stream encoding} {
        for {set j 0} {$j < 1000} {incr j} {
            if {rand() < 0.9} {
                r xadd stream * foo abc
            } else {
                r xadd stream * bar $j
            }
        }
        r xgroup create stream mygroup 0
        set records [r xreadgroup GROUP mygroup Alice COUNT 2 STREAMS stream >]
        r xdel stream [lindex [lindex [lindex [lindex $records 0] 1] 1] 0]
        r xack stream mygroup [lindex [lindex [lindex [lindex $records 0] 1] 0] 0]
        set digest [debug_digest]
        r config set sanitize-dump-payload no
        r debug reload
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
    }
    test {Test RDB stream encoding - sanitize dump} {
        r config set sanitize-dump-payload yes
        r debug reload
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
    }
    # delete the stream, maybe valgrind will find something
    r del stream
}

set dump_path [file join $server_path dump.rdb]

# Prepare custom umask test scenario
if {[catch {package require Tclx}]} {
    if {$::verbose} {
        puts "Skipping umask test. Package Tclx not installed."
    }
} else {
    # We have umask from the Tclx package.
    set old_umask [umask]
    set old_perm [expr {666 - $old_umask}]
    assert_equal [file attributes $dump_path -permissions] 00$old_perm

    if {$old_umask == 22} {
        set new_umask 2
    } else {
        set new_umask 22
    }
    set new_perm [expr {666 - $new_umask}]

    umask $new_umask
    start_server [list overrides [list "dir" $server_path] keep_persistence true] {
        test {Test nondefault umask applied} {
            r save
            # Use numeric comparison for compatibility with Tcl 8 and 9.
            assert_range [file attributes $dump_path -permissions] 00$new_perm 00$new_perm
        }
    }
    umask $old_umask
}

# Make the RDB file unreadable
file attributes $dump_path -permissions 0222

# Detect root account (it is able to read the file even with 002 perm)
set isroot 0
catch {
    open $dump_path
    set isroot 1
}

# Now make sure the server aborted with an error
if {!$isroot} {
    start_server_and_kill_it [list "dir" $server_path] {
        test {Server should not start if RDB file can't be open} {
            wait_for_condition 50 100 {
                [string match {*Fatal error loading*} \
                    [exec tail -1 < [dict get $srv stdout]]]
            } else {
                fail "Server started even if RDB was unreadable!"
            }
        }
    }
}

# Fix permissions of the RDB file.
file attributes $dump_path -permissions 0666

# Corrupt its CRC64 checksum.
set filesize [file size $dump_path]
set fd [open $dump_path r+]
fconfigure $fd -translation binary
seek $fd -8 end
puts -nonewline $fd "foobar00"; # Corrupt the checksum
close $fd

# Now make sure the server aborted with an error
start_server_and_kill_it [list "dir" $server_path] {
    test {Server should not start if RDB is corrupted} {
        wait_for_condition 50 100 {
            [string match {*CRC error*} \
                [exec tail -10 < [dict get $srv stdout]]]
        } else {
            fail "Server started even if RDB was corrupted!"
        }
    }
}

start_server {overrides {forkless-options-supported yes}} {
    foreach bgsave_type {"" "fork" "thread"} {
        test "Test FLUSHALL aborts bgsave $bgsave_type" {
            r config set save ""
            # 5000 keys with 1ms sleep per key should take 5 second
            r config set rdb-key-save-delay 1000
            populate 5000
            assert_lessthan 999 [s rdb_changes_since_last_save]
            r bgsave {*}$bgsave_type
            assert_equal [s rdb_bgsave_in_progress] 1
            
            # Verify we're testing the right save type while it's running
            set expected_type [expr {$bgsave_type eq "thread" ? "thread" : "fork"}]
            assert_equal [s rdb_current_bgsave_type] $expected_type
            
            r flushall
            # wait a second max (bgsave should take 5)
            wait_for_condition 10 100 {
                [s rdb_bgsave_in_progress] == 0
            } else {
                fail "bgsave not aborted"
            }
            # verify that bgsave failed, by checking that the change counter is still high
            assert_lessthan 999 [s rdb_changes_since_last_save]
            # make sure the server is still writable
            r set x xx
        }
    }

    foreach bgsave_type {"" "fork" "thread"} {
        test "bgsave $bgsave_type resets the change counter" {
            r config set rdb-key-save-delay 0
            r bgsave {*}$bgsave_type
            wait_for_condition 50 100 {
                [s rdb_bgsave_in_progress] == 0
            } else {
                fail "bgsave not done"
            }
            assert_equal [s rdb_changes_since_last_save] 0
            
            # Verify we tested the right save type
            set expected_type [expr {$bgsave_type eq "thread" ? "thread" : "fork"}]
            assert_equal [s rdb_last_bgsave_type] $expected_type
        }
    }

    foreach bgsave_type {"" "fork" "thread"} {
        test "bgsave cancel aborts $bgsave_type save" {
            r config set save ""
            # Generating RDB will take some 100 seconds
            r config set rdb-key-save-delay 1000000
            populate 100 "" 16

            r bgsave {*}$bgsave_type
            wait_for_condition 50 100 {
                [s rdb_bgsave_in_progress] == 1
            } else {
                fail "bgsave did not start in time"
            }
            
            # Verify we're testing the right save type
            set expected_type [expr {$bgsave_type eq "thread" ? "thread" : "fork"}]
            assert_equal [s rdb_current_bgsave_type] $expected_type
            
            if {$bgsave_type ne "thread"} {
                set fork_child_pid [get_child_pid 0]
            }
            
            assert {[r bgsave cancel] eq {Background saving cancelled}}
            
            if {$bgsave_type ne "thread"} {
                set temp_rdb [file join [lindex [r config get dir] 1] temp-${fork_child_pid}.rdb]
                # Temp rdb must be deleted
                wait_for_condition 50 100 {
                    ![file exists $temp_rdb]
                } else {
                    fail "bgsave temp file was not deleted after cancel"
                }
            }

             # Make sure no save is running and that bgsave return an error
             wait_for_condition 50 100 {
                [s rdb_bgsave_in_progress] == 0
            } else {
                fail "bgsave is currently running"
            }
            assert_error "ERR Background saving is currently not in progress or scheduled" {r bgsave cancel}
        }
    }

    test {bgsave cancel schedulled request} {
        r config set save ""
        # Generating RDB will take some 100 seconds
        r config set rdb-key-save-delay 1000000
        populate 100 "" 16

        # start a long AOF child
        r bgrewriteaof
        wait_for_condition 50 100 {
            [s aof_rewrite_in_progress] == 1
        } else {
            fail "aof not started"
        }
        
        # Make sure cancel return valid status
        assert {[r bgsave schedule] eq {Background saving scheduled}}

        # Cancel the scheduled save
        assert {[r bgsave cancel] eq {Scheduled background saving cancelled}}

        # Make sure a second call to bgsave cancel return an error
        assert_error "ERR Background saving is currently not in progress or scheduled" {r bgsave cancel}
        
        # Cleanup: speed up and wait for AOF rewrite to finish
        r config set rdb-key-save-delay 0
        waitForBgrewriteaof r
    }

    test "thread bgsave contains expired keys from when save started" {
        r flushdb
        # Disable automatic saves
        r config set save ""
        
        # Set two keys that expire together
        r set k1 v1
        r set k2 v2
        set curr_time [clock seconds]
        r expireat k1 [expr {$curr_time + 2}]
        r expireat k2 [expr {$curr_time + 2}]
        
        # Start slow thread save
        r config set rdb-key-save-delay 100000
        r bgsave thread
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "thread bgsave did not start"
        }
        
        # Let both keys expire
        after 3000
        
        # Serialize k1 in the foreground by touching it
        r set k1 v11
        
        # Complete threadsave so k2 will be serialized in background
        r config set rdb-key-save-delay 0
        waitForBgsave r
        
        # Check both keys are in the RDB
        set rdb_path [file join [lindex [r config get dir] 1] [lindex [r config get dbfilename] 1]]
        set fd [open $rdb_path rb]
        set rdb_content [read $fd]
        close $fd
        assert {[string first "k1" $rdb_content] != -1}
        assert {[string first "k2" $rdb_content] != -1}
    } {} {needs:debug}

    test "FLUSHDB during single-db thread bgsave causes save to fail" {
        r flushall
        r config set save ""
        
        # Populate database with complex dataset
        createComplexDataset r 100
        
        # Get initial key count
        set initial_keys [r dbsize]
        assert {$initial_keys > 0}
        
        # Start threadsave with very slow save (high delay per key)
        r config set rdb-key-save-delay 100000
        r bgsave thread
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "thread bgsave did not start"
        }
        
        # FLUSHDB should cancel the save
        r flushdb
        assert_equal [r dbsize] 0
        
        # Speed up and wait for save to abort
        # Note: Cancellation needs to be processed by background thread
        r config set rdb-key-save-delay 0
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 0
        } else {
            fail "thread bgsave did not abort"
        }
        
        # Verify save failed
        assert_equal [s rdb_last_bgsave_status] err
        assert_equal [s rdb_last_bgsave_type] thread
    } {} {needs:debug}

    test "FLUSHDB during multi-db thread bgsave causes save to fail" {
        # Disable automatic saves
        r config set save ""
        r select 0
        r flushdb
        
        # Populate multiple databases
        for {set i 0} {$i < 100} {incr i} {
            r set key$i val$i
        }
        r select 1
        for {set i 0} {$i < 100} {incr i} {
            r set key$i val$i
        }
        r select 0
        
        # Start slow thread save
        r config set rdb-key-save-delay 100000
        r bgsave thread
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "thread bgsave did not start"
        }
        
        # Give threadsave time to start iterating
        after 100
        
        # FLUSHDB on db 1 while save is running - this should terminate the save
        r select 1
        r flushdb
        r select 0
        
        # Wait for save to abort
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 0
        } else {
            fail "thread bgsave did not abort"
        }
        
        # Threadsave should have failed
        assert_equal [s rdb_last_bgsave_status] err
    } {} {needs:debug}

    test "multiple databases modifications during thread bgsave" {
        r flushall
        r config set save ""
        
        # Populate 5 databases with all data types
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            createComplexDatasetForVerification r 20 "db${db}_"
        }
        r select 0
        
        # Start slow threadsave
        r config set rdb-key-save-delay 100000
        r bgsave thread
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "thread bgsave did not start"
        }
        
        # Modify keys in all databases while save is running
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            # Modify string keys
            for {set i 0} {$i < 20} {incr i} {
                r append db${db}_before_$i "_MODIFIED"
            }
            # Modify list keys
            for {set i 0} {$i < 20} {incr i} {
                r lpush db${db}_lst_$i "MODIFIED"
            }
            # Modify set keys
            for {set i 0} {$i < 20} {incr i} {
                r sadd db${db}_set_$i "MODIFIED"
            }
        }
        r select 0
        
        # Verify modifications happened in live database
        assert {[s rdb_changes_since_last_save] > 0}
        r select 0
        assert_match "*MODIFIED*" [r get db0_before_0]
        
        # Speed up and complete save
        r config set rdb-key-save-delay 0
        waitForBgsave r
        
        # Verify save completed successfully
        assert_equal [s rdb_last_bgsave_status] ok
        
        # Reload from RDB and verify ORIGINAL values are preserved
        # (consistent snapshot should capture state at start of save)
        r debug reload
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            # Check that we have original values, not modified ones
            set val [r get db${db}_before_0]
            assert_match "value_before_0" $val
            assert_no_match "*MODIFIED*" $val
            
            set list_items [r lrange db${db}_lst_0 0 -1]
            assert {[lsearch $list_items "L1"] != -1}
            assert {[lsearch $list_items "MODIFIED"] == -1}
            
            set set_members [r smembers db${db}_set_0]
            assert {[lsearch $set_members "B1"] != -1}
            assert {[lsearch $set_members "MODIFIED"] == -1}
        }
        r select 0
    } {} {needs:debug}

    test "modify new keys during thread bgsave" {
        r flushall
        r config set save ""
        
        # Populate database with all data types
        createComplexDatasetForVerification r 20
        set original_keys [r dbsize]
        
        # Start threadsave with very slow save (high delay per key)
        r config set rdb-key-save-delay 100000
        r bgsave thread
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "thread bgsave did not start"
        }
        
        # Create new keys with different data types while save is running
        for {set i 0} {$i < 50} {incr i} {
            r set after_str_$i "value_$i"
            r lpush after_list_$i "L1" "L2"
            r sadd after_set_$i "S1" "S2"
            r zadd after_zset_$i 1 "Z1" 2 "Z2"
            r hset after_hash_$i "H1" "V1"
        }
        
        # Now MODIFY the new keys
        for {set i 0} {$i < 50} {incr i} {
            r append after_str_$i "_modified"
            r lpush after_list_$i "L3"
            r sadd after_set_$i "S3"
            r zadd after_zset_$i 3 "Z3"
            r hset after_hash_$i "H2" "V2"
        }
        
        # Verify new keys were created and modified, save still in progress
        set new_key_count [r dbsize]
        assert {$new_key_count > $original_keys}
        assert_equal [s rdb_bgsave_in_progress] 1
        assert_match "*modified*" [r get after_str_0]
        
        # Speed up and complete save
        r config set rdb-key-save-delay 0
        waitForBgsave r
        
        # Verify save completed successfully
        assert_equal [s rdb_last_bgsave_status] ok
        
        # Reload and verify ONLY original keys exist (new keys should NOT be in snapshot)
        # Consistent snapshot should only have keys that existed at start
        r debug reload
        assert_equal [r dbsize] $original_keys
        
        # Verify original keys exist (sample check)
        assert_equal [r get before_0] "value_before_0"
        assert_equal [r lrange lst_0 0 -1] [list "R2" "R1" "L1" "L2"]
        assert_equal [lsort [r smembers set_0]] [list "B1" "B2"]
        
        # Verify new keys do NOT exist in snapshot
        assert_equal [r exists after_str_0] 0
        assert_equal [r exists after_list_0] 0
    } {} {needs:debug}

    test "SWAPDB during thread bgsave" {
        r flushall
        r config set save ""
        
        # Populate 5 databases with all data types
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            createComplexDatasetForVerification r 20 "db${db}_"
        }
        r select 0
        
        # Get initial key count per database
        array set initial_db_keys {}
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            set initial_db_keys($db) [r dbsize]
        }
        r select 0
        
        # Start slow threadsave
        r config set rdb-key-save-delay 100000
        r bgsave thread
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "thread bgsave did not start"
        }
        
        # Keep swapping databases until save completes
        set perm [list 0 1 2 3 4]
        set swaps 0
        while {[s rdb_bgsave_in_progress] == 1 && $swaps < 200} {
            incr swaps
            # Shuffle permutation
            for {set i 4} {$i > 0} {incr i -1} {
                set j [expr {int(rand() * ($i + 1))}] ;# j is a random index from 0 to i, inclusive
                set temp [lindex $perm $i]
                lset perm $i [lindex $perm $j]
                lset perm $j $temp
            }
            # Swap each database with its permuted target
            for {set db 0} {$db < 5} {incr db} {
                r swapdb $db [lindex $perm $db]
            }
            
            # Speed up save after some swaps
            if {$swaps == 50} {
                r config set rdb-key-save-delay 0
            }
        }
        
        # Wait for save to complete if still running
        if {[s rdb_bgsave_in_progress] == 1} {
            r config set rdb-key-save-delay 0
            waitForBgsave r
        }
        
        # Verify save completed successfully
        assert_equal [s rdb_last_bgsave_status] ok
        
        # Reload from RDB and verify keys are in ORIGINAL databases
        # (SWAPDB is ignored for consistent snapshots)
        r select 0
        r debug reload
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            # Each database should have its original key count
            assert_equal [r dbsize] $initial_db_keys($db)
            # Verify original values in original database (sample check)
            assert_equal [r get db${db}_before_0] "value_before_0"
            assert_equal [r lrange db${db}_lst_0 0 -1] [list "R2" "R1" "L1" "L2"]
            assert_equal [lsort [r smembers db${db}_set_0]] [list "B1" "B2"]
            assert_equal [r hget db${db}_hash_0 "H1"] "a"
        }
        r select 0
    } {} {needs:debug}

    test "delete all keys after SWAPDB during thread bgsave" {
        r flushall
        r config set save ""
        
        # Populate 5 databases with all data types
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            createComplexDatasetForVerification r 20 "db${db}_"
        }
        r select 0
        
        # Get initial key count
        set initial_keys 0
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            incr initial_keys [r dbsize]
        }
        r select 0
        
        # Start slow threadsave
        r config set rdb-key-save-delay 100000
        r bgsave thread
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "thread bgsave did not start"
        }
        
        # Swap databases with fixed permutation [2, 3, 4, 0, 1]
        set perm [list 2 3 4 0 1]
        for {set db 0} {$db < 5} {incr db} {
            r swapdb $db [lindex $perm $db]
        }
        
        # Delete all keys in all databases
        for {set db 4} {$db >= 0} {incr db -1} {
            r select $db
            set keys [r keys *]
            foreach key $keys {
                r del $key
            }
            assert_equal [r dbsize] 0
        }
        
        # Speed up and complete save
        r config set rdb-key-save-delay 0
        waitForBgsave r
        
        # Verify save completed successfully
        assert_equal [s rdb_last_bgsave_status] ok
        
        # Reload from RDB and verify ORIGINAL keys still exist
        # Consistent snapshot should preserve state before SWAPDB and deletions
        r select 0
        r debug reload
        set total_keys 0
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            incr total_keys [r dbsize]
        }
        assert_equal $total_keys $initial_keys
        
        # Verify original values exist in original databases (sample check)
        for {set db 0} {$db < 5} {incr db} {
            r select $db
            assert_equal [r get db${db}_before_0] "value_before_0"
            assert_equal [r lrange db${db}_lst_0 0 -1] [list "R2" "R1" "L1" "L2"]
        }
        r select 0
    } {} {needs:debug}

    test "deleting keys during thread bgsave" {
        r flushall
        r config set save ""
        
        # Populate database with all data types
        createComplexDatasetForVerification r 20
        set initial_keys [r dbsize]
        assert {$initial_keys > 0}
        
        # Start threadsave with very slow save (high delay per key)
        r config set rdb-key-save-delay 100000
        r bgsave thread
        wait_for_condition 50 100 {
            [s rdb_bgsave_in_progress] == 1
        } else {
            fail "thread bgsave did not start"
        }
        
        # Delete all keys in the database
        set keys [r keys *]
        foreach key $keys {
            r del $key
        }
        
        # Verify all keys deleted and save still in progress
        assert_equal [r dbsize] 0
        assert_equal [s rdb_bgsave_in_progress] 1
        
        # Speed up and complete save
        r config set rdb-key-save-delay 0
        waitForBgsave r
        
        # Verify save completed successfully
        assert_equal [s rdb_last_bgsave_status] ok
        
        # Reload from RDB and verify ORIGINAL keys still exist
        # Consistent snapshot should preserve keys that existed at start
        r debug reload
        assert_equal [r dbsize] $initial_keys
        
        # Verify original keys exist (sample check)
        assert_equal [r get before_0] "value_before_0"
        assert_equal [r lrange lst_0 0 -1] [list "R2" "R1" "L1" "L2"]
        assert_equal [r smembers set_0] [lsort [list "B1" "B2"]]
    } {} {needs:debug}

    foreach first_type {fork thread} {
        foreach second_type {fork thread} {
            test "$first_type bgsave blocks $second_type bgsave" {
                r config set save "" ;# Disable automatic saves
                r config set rdb-key-save-delay 1000000
                populate 100 "" 16

                r bgsave $first_type
                wait_for_condition 50 100 {
                    [s rdb_bgsave_in_progress] == 1
                } else {
                    fail "$first_type bgsave did not start"
                }
                assert_equal [s rdb_current_bgsave_type] $first_type

                assert_error "ERR Background save already in progress" {r bgsave $second_type}
                
                r bgsave cancel
                waitForBgsave r
            }
        }
    }


}

test {client freed during loading} {
    start_server [list overrides [list key-load-delay 50 loading-process-events-interval-bytes 1024 rdbcompression no save "900 1"]] {
        # create a big rdb that will take long to load. it is important
        # for keys to be big since the server processes events only once in 2mb.
        # 100mb of rdb, 100k keys will load in more than 5 seconds
        r debug populate 100000 key 1000

        restart_server 0 false false

        # make sure it's still loading
        assert_equal [s loading] 1

        # connect and disconnect 5 clients
        set clients {}
        for {set j 0} {$j < 5} {incr j} {
            lappend clients [valkey_deferring_client]
        }
        foreach rd $clients {
            $rd debug log bla
        }
        foreach rd $clients {
            $rd read
        }
        foreach rd $clients {
            $rd close
        }

        # make sure the server freed the clients
        wait_for_condition 100 100 {
            [s connected_clients] < 3
        } else {
            fail "clients didn't disconnect"
        }

        # make sure it's still loading
        assert_equal [s loading] 1

        # no need to keep waiting for loading to complete
        exec kill [srv 0 pid]
    }
}

start_server {} {
    test {Test RDB load info} {
        r debug populate 1000
        r save
        assert {[r lastsave] <= [lindex [r time] 0]}
        restart_server 0 true false
        wait_done_loading r
        assert {[s rdb_last_load_keys_expired] == 0}
        assert {[s rdb_last_load_keys_loaded] == 1000}

        r debug set-active-expire 0
        for {set j 0} {$j < 1024} {incr j} {
            r select [expr $j%16]
            r set $j somevalue px 10
        }
        after 20

        r save
        restart_server 0 true false
        wait_done_loading r
        assert {[s rdb_last_load_keys_expired] == 1024}
        assert {[s rdb_last_load_keys_loaded] == 1000}
    }
}

# Our COW metrics (Private_Dirty) work only on Linux
set system_name [string tolower [exec uname -s]]
set page_size [exec getconf PAGESIZE]
if {$system_name eq {linux} && $page_size == 4096} {

start_server {overrides {save ""}} {
    test {Test child sending info} {
        # make sure that rdb_last_cow_size and current_cow_size are zero (the test using new server),
        # so that the comparisons during the test will be valid
        assert {[s current_cow_size] == 0}
        assert {[s current_save_keys_processed] == 0}
        assert {[s current_save_keys_total] == 0}

        assert {[s rdb_last_cow_size] == 0}

        # using a 200us delay, the bgsave is empirically taking about 10 seconds.
        # we need it to take more than some 5 seconds, since the server only report COW once a second.
        r config set rdb-key-save-delay 200
        r config set loglevel debug

        # populate the db with 10k keys of 512B each (since we want to measure the COW size by
        # changing some keys and read the reported COW size, we are using small key size to prevent from
        # the "dismiss mechanism" free memory and reduce the COW size)
        set rd [valkey_deferring_client 0]
        set size 500 ;# aim for the 512 bin (sds overhead)
        set cmd_count 10000
        for {set k 0} {$k < $cmd_count} {incr k} {
            $rd set key$k [string repeat A $size]
        }

        for {set k 0} {$k < $cmd_count} {incr k} {
            catch { $rd read }
        }

        $rd close

        # start background rdb save
        r bgsave

        set current_save_keys_total [s current_save_keys_total]
        if {$::verbose} {
            puts "Keys before bgsave start: $current_save_keys_total"
        }

        # on each iteration, we will write some key to the server to trigger copy-on-write, and
        # wait to see that it reflected in INFO.
        set iteration 1
        set key_idx 0
        while 1 {
            # take samples before writing new data to the server
            set cow_size [s current_cow_size]
            if {$::verbose} {
                puts "COW info before copy-on-write: $cow_size"
            }

            set keys_processed [s current_save_keys_processed]
            if {$::verbose} {
                puts "current_save_keys_processed info : $keys_processed"
            }

            # trigger copy-on-write
            set modified_keys 16
            for {set k 0} {$k < $modified_keys} {incr k} {
                r setrange key$key_idx 0 [string repeat B $size]
                incr key_idx 1
            }

            # changing 16 keys (512B each) will create at least 8192 COW (2 pages), but we don't want the test
            # to be too strict, so we check for a change of at least 4096 bytes
            set exp_cow [expr $cow_size + 4096]
            # wait to see that current_cow_size value updated (as long as the child is in progress)
            wait_for_condition 80 100 {
                [s rdb_bgsave_in_progress] == 0 ||
                [s current_cow_size] >= $exp_cow &&
                [s current_save_keys_processed] > $keys_processed &&
                [s current_fork_perc] > 0
            } else {
                if {$::verbose} {
                    puts "COW info on fail: [s current_cow_size]"
                    puts [exec tail -n 100 < [srv 0 stdout]]
                }
                fail "COW info wasn't reported"
            }

            # assert that $keys_processed is not greater than total keys.
            assert_morethan_equal $current_save_keys_total $keys_processed

            # for no accurate, stop after 2 iterations
            if {!$::accurate && $iteration == 2} {
                break
            }

            # stop iterating if the bgsave completed
            if { [s rdb_bgsave_in_progress] == 0 } {
                break
            }

            incr iteration 1
        }

        # make sure we saw report of current_cow_size
        if {$iteration < 2 && $::verbose} {
            puts [exec tail -n 100 < [srv 0 stdout]]
        }
        assert_morethan_equal $iteration 2

        # if bgsave completed, check that rdb_last_cow_size (fork exit report)
        # is at least 90% of last rdb_active_cow_size.
        if { [s rdb_bgsave_in_progress] == 0 } {
            set final_cow [s rdb_last_cow_size]
            set cow_size [expr $cow_size * 0.9]
            if {$final_cow < $cow_size && $::verbose} {
                puts [exec tail -n 100 < [srv 0 stdout]]
            }
            assert_morethan_equal $final_cow $cow_size
        }
    }
}
} ;# system_name

exec cp -f tests/assets/scriptbackup.rdb $server_path
start_server [list overrides [list "dir" $server_path "dbfilename" "scriptbackup.rdb" "appendonly" "no"]] {
    # the script is: "return redis.call('set', 'foo', 'bar')""
    # its sha1   is: a0c38691e9fffe4563723c32ba77a34398e090e6
    test {script won't load anymore if it's in rdb} {
        assert_equal [r script exists a0c38691e9fffe4563723c32ba77a34398e090e6] 0
    }
}

start_server {overrides {forkless-options-supported yes}} {
    foreach bgsave_type {"" "fork" "thread"} {
        test "failed bgsave $bgsave_type prevents writes" {
            # Make sure the server saves an RDB on shutdown
            r config set save "900 1"

            r config set rdb-key-save-delay 10000000
            populate 1000
            r set x x
            r bgsave {*}$bgsave_type
            
            if {$bgsave_type ne "thread"} {
                set pid1 [get_child_pid 0]
                catch {exec kill -9 $pid1}
            } else {
                # For threadsave, cancel it to simulate failure
                r bgsave cancel
            }
            waitForBgsave r

            # make sure a read command succeeds
            assert_equal [r get x] x

            # make sure a write command fails
            assert_error {MISCONF *} {r set x y}

            # repeat with script
            assert_error {MISCONF *} {r eval {
                return redis.call('set','x',1)
                } 1 x
            }
            assert_equal {x} [r eval {
                return redis.call('get','x')
                } 1 x
            ]

            # again with script using shebang
            assert_error {MISCONF *} {r eval {#!lua
                return redis.call('set','x',1)
                } 1 x
            }
            assert_equal {x} [r eval {#!lua flags=no-writes
                return redis.call('get','x')
                } 1 x
            ]

            r config set rdb-key-save-delay 0
            r bgsave {*}$bgsave_type
            waitForBgsave r

            # server is writable again
            r set x y
        } {OK}
    }
}

start_server {} {
    test {RDB Load from incompatible version preserves data} {
        # Set test keys
        r set testkey1 "value1"
        r set testkey2 "value2" 

        # Use RDB with version 987. 
        # This emulates a full sync from a server with a future version
        set server_dir [lindex [r config get dir] 1]
        set rdb_filename [lindex [r config get dbfilename] 1]
        set rdb_path "$server_dir/$rdb_filename"
        exec cp tests/assets/encodings-rdb987.rdb $rdb_path

        # Reload will trigger the rdbLoad code path with the RDBFLAGS_EMPTY_DATA flag
        catch {r debug reload nosave}
        
        # Check that version error appears in logs
        verify_log_message 0 "*Can't handle RDB format version*" 0

        # Verify we don't enter the flushing code path
        verify_no_log_message 0 "*RDB signature and version check passed*" 0

        # Verify our original data is not flushed
        assert_equal [r get testkey1] "value1"
        assert_equal [r get testkey2] "value2"
    }
}

} ;# tags
