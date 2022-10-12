## MIT 6.824 Distributed Systems: Lab 4: Sharded Key/Value Service - Part B: Sharded Key/Value Server

Solution for [Lab 4B: Sharded Key/Value Server](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

Tests:
```sh
❯ time python3 mtests.py --workers 4 -n 120 4B
┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                     ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestStaticShards         │      0 │   120 │  4.51 ± 0.17 │
│ TestJoinLeave            │      0 │   120 │  3.05 ± 0.28 │
│ TestSnapshot             │      0 │   120 │  6.13 ± 0.31 │
│ TestMissChange           │      0 │   120 │  9.29 ± 0.54 │
│ TestConcurrent1          │      0 │   120 │  5.12 ± 0.24 │
│ TestConcurrent2          │      0 │   120 │ 15.09 ± 0.32 │
│ TestConcurrent3          │      0 │   120 │ 17.36 ± 0.81 │
│ TestUnreliable1          │      0 │   120 │ 10.29 ± 1.29 │
│ TestUnreliable2          │      0 │   120 │  8.26 ± 0.45 │
│ TestUnreliable3          │      0 │   120 │  8.17 ± 0.46 │
│ TestChallenge1Delete     │      0 │   120 │ 18.59 ± 0.24 │
│ TestChallenge2Unaffected │      0 │   120 │  7.21 ± 0.76 │
│ TestChallenge2Partial    │      0 │   120 │  5.67 ± 0.53 │
└──────────────────────────┴────────┴───────┴──────────────┘
9697.86s user 1918.05s system 325% cpu 59:32.82 total
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
