## MIT 6.824 Distributed Systems: Lab 3 Fault-tolerant Key/Value Service 

Solution for [Lab 3: Fault-tolerant Key/Value Service](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)

Tests:
```sh
time python3 mtests.py --workers 4 -n 40 TestStaticShards TestJoinLeave TestSnapshot TestMissChange
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test             ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestStaticShards │      0 │    40 │  6.95 ± 0.35 │
│ TestJoinLeave    │      0 │    40 │  6.42 ± 0.48 │
│ TestSnapshot     │      0 │    40 │ 13.61 ± 1.24 │
│ TestMissChange   │      0 │    40 │ 17.54 ± 4.67 │
└──────────────────┴────────┴───────┴──────────────┘
1511.51s user 202.18s system 381% cpu 7:29.12 total
```

Environment:
```sh
❯ cat /etc/lsb-release
DISTRIB_ID=Ubuntu
DISTRIB_RELEASE=22.04
DISTRIB_CODENAME=jammy
DISTRIB_DESCRIPTION="Ubuntu 22.04 LTS"

❯ cat /proc/cpuinfo | egrep 'processor|venfor_id|model|cpu MHz|cache|cpu cores' | sort | uniq
cache_alignment : 64
cache size      : 6144 KB
cpu cores       : 1
cpu MHz         : 2400.000
model           : 142
model name      : Intel(R) Core(TM) i5-8279U CPU @ 2.40GHz
processor       : 0
processor       : 1
processor       : 2
processor       : 3
```
