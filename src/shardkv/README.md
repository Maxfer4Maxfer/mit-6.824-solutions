## MIT 6.824 Distributed Systems: Lab 4: Sharded Key/Value Service - Part A: The Shard controller

Solution for [Lab 4A: The Shard controller](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

Tests:
```sh
time python3 mtests.py --workers 4 -n 40 TestStaticShards TestJoinLeave TestSnapshot TestMissChange
┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                     ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestStaticShards         │      0 │     4 │  7.51 ± 0.68 │
│ TestJoinLeave            │      0 │     4 │  6.67 ± 0.49 │
│ TestSnapshot             │      0 │     4 │ 15.82 ± 0.91 │
│ TestMissChange           │      0 │     4 │ 18.60 ± 2.01 │
│ TestChallenge1Delete     │      0 │     4 │ 22.46 ± 0.62 │
│ TestChallenge2Unaffected │      0 │     4 │  9.75 ± 0.66 │
│ TestChallenge2Partial    │      0 │     4 │  6.72 ± 0.39 │
└──────────────────────────┴────────┴───────┴──────────────┘
304.06s user 41.28s system 378% cpu 1:31.19 total
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
