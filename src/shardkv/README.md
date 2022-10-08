## MIT 6.824 Distributed Systems: Lab 4: Sharded Key/Value Service - Part B: Sharded Key/Value Server

Solution for [Lab 4B: Sharded Key/Value Server](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

Tests:
```sh
❯ time python3 mtests.py --workers 4 -n 40 4B
┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                     ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestStaticShards         │      0 │    40 │  4.37 ± 0.24 │
│ TestJoinLeave            │      0 │    40 │  3.04 ± 0.26 │
│ TestSnapshot             │      0 │    40 │  6.15 ± 0.41 │
│ TestMissChange           │      0 │    40 │  9.11 ± 0.55 │
│ TestConcurrent1          │      0 │    40 │  5.07 ± 0.21 │
│ TestConcurrent2          │      0 │    40 │ 15.00 ± 0.24 │
│ TestConcurrent3          │      0 │    40 │ 17.05 ± 0.76 │
│ TestUnreliable1          │      0 │    40 │ 10.08 ± 1.37 │
│ TestUnreliable2          │      0 │    40 │  8.29 ± 0.42 │
│ TestUnreliable3          │      0 │    40 │  8.18 ± 0.49 │
│ TestChallenge1Delete     │      0 │    40 │ 18.64 ± 0.27 │
│ TestChallenge2Unaffected │      0 │    40 │  7.30 ± 0.77 │
│ TestChallenge2Partial    │      0 │    40 │  5.58 ± 0.58 │
└──────────────────────────┴────────┴───────┴──────────────┘
3233.35s user 641.11s system 326% cpu 19:45.94 total
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
