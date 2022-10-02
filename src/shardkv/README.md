## MIT 6.824 Distributed Systems: Lab 4: Sharded Key/Value Service - Part A: The Shard controller

Solution for [Lab 4A: The Shard controller](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

Tests:
```sh
┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                     ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestStaticShards         │      0 │    40 │  8.05 ± 0.48 │
│ TestJoinLeave            │      0 │    40 │  7.27 ± 0.52 │
│ TestSnapshot             │      0 │    40 │ 17.05 ± 1.75 │
│ TestMissChange           │      0 │    40 │ 21.25 ± 2.47 │
│ TestChallenge1Delete     │      0 │    40 │ 24.20 ± 0.85 │
│ TestChallenge2Unaffected │      0 │    40 │  9.68 ± 1.18 │
│ TestChallenge2Partial    │      0 │    40 │  8.11 ± 0.92 │
└──────────────────────────┴────────┴───────┴──────────────┘
3224.32s user 442.94s system 380% cpu 16:03.04 total
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
