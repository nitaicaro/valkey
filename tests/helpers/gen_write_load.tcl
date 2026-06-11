source tests/support/valkey.tcl

set ::tlsdir "tests/tls"

# Continuously sends SET commands to the node. If key is omitted, a random key is
# used for every SET command. The value is always random.
proc gen_write_load {host port seconds tls db {key ""}} {
    set start_time [clock seconds]
    set r [valkey $host $port 1 $tls]
    $r client setname LOAD_HANDLER
    if {$db != 0} {
        $r select $db
    }

    # Pre-compute mode and constants outside the loop
    if {$key == ""} {
        set mode random
    } elseif {[string match "*:*" $key]} {
        set mode overwrite
        set parts [split $key ":"]
        set prefix [lindex $parts 0]
        set count [lindex $parts 1]
        switch $prefix {
            small   {set val [string repeat "x" 1024]}
            medium  {set val [string repeat "x" 10240]}
            large   {set val [string repeat "x" 102400]}
            xlarge  {set val [string repeat "x" 1048576]}
            default {set val [string repeat "x" 1024]}
        }
    } else {
        set mode single
    }

    set count_ops 0
    set logfile "/tmp/gen_write_load_[pid].log"
    set fp [open $logfile w]

    while 1 {
        switch $mode {
            random    { $r set [expr rand()] [expr rand()] }
            overwrite {
                set k [format "%s:%09d" $prefix [expr {int(rand()*$count)}]]
                $r set $k $val
                if {$count_ops < 10} { puts $fp "SET $k (len=[string length $val])"; flush $fp }
            }
            single    { $r set $key [expr rand()] }
        }
        incr count_ops
        if {[clock seconds]-$start_time > $seconds} {
            puts $fp "Total ops: $count_ops in $seconds seconds"
            close $fp
            exit 0
        }
    }
}

gen_write_load [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4] [lindex $argv 5]
