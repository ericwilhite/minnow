#!/bin/sh
# wsample.sh ROOTPID OUT: every 10 s, each descendant over 1 GB RSS: pid, RSS MB, age s.
desc(){ for c in $(cat /proc/$1/task/*/children 2>/dev/null); do echo $c; desc $c; done; }
while [ -d /proc/$1 ]; do
  for d in $(desc $1); do r=$(awk '/VmRSS/{print $2}' /proc/$d/status 2>/dev/null); [ -n "$r" ] && [ "$r" -gt 1000000 ] && echo "$(date +%s) $d $((r/1024)) $(ps -o etimes= -p $d)" >> $2; done; sleep 10
done
