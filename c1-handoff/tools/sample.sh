#!/bin/sh
# sample.sh ROOTPID OUT: every 5 s, the largest RSS among ROOTPID's descendants (kB) and its age.
desc(){ for c in $(cat /proc/$1/task/*/children 2>/dev/null); do echo $c; desc $c; done; }
while [ -d /proc/$1 ]; do
  m=0; a=0; for d in $(desc $1); do r=$(awk '/VmRSS/{print $2}' /proc/$d/status 2>/dev/null); [ -n "$r" ] && [ "$r" -gt "$m" ] && { m=$r; a=$(ps -o etimes= -p $d); }; done
  echo "$(date +%s) $m $a" >> $2; sleep 5
done
