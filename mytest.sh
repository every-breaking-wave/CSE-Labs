#!/usr/bin/env bash



  ./stop.sh
  make clean
  rm ./log/logdata.bin
  make
  ./start.sh
  perl ./test-lab2a-part1-a.pl chfs1
  rm ./log/logdata.bin


