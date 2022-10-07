## MIT 6.824 Distributed Systems: Lab 4: Sharded Key/Value Service - Part A: The Shard controller

Solution for [Lab 4A: The Shard controller](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

Tests:
```sh
❯ time python3 mtests.py --workers 4 -n 40 TestStaticShards TestJoinLeave TestSnapshot TestMissChange TestConcurrent1 TestConcurrent2 TestUnreliable1 TestUnreliable2 TestUnreliable3  TestChallenge1Delete TestChallenge2Unaffected TestChallenge2Partial
┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                     ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestStaticShards         │      0 │    40 │  8.34 ± 0.78 │
│ TestJoinLeave            │      0 │    40 │  7.94 ± 0.80 │
│ TestSnapshot             │      0 │    40 │ 19.96 ± 1.56 │
│ TestMissChange           │      0 │    40 │ 22.40 ± 2.74 │
│ TestConcurrent1          │      0 │    40 │ 13.65 ± 6.36 │
│ TestConcurrent2          │      0 │    40 │ 25.19 ± 2.69 │
│ TestUnreliable1          │      0 │    40 │ 23.63 ± 5.58 │
│ TestUnreliable2          │      0 │    40 │ 12.87 ± 1.58 │
│ TestUnreliable3          │      0 │    40 │ 12.66 ± 2.68 │
│ TestChallenge1Delete     │      0 │    40 │ 28.10 ± 2.42 │
│ TestChallenge2Unaffected │      0 │    40 │ 11.85 ± 2.00 │
│ TestChallenge2Partial    │      0 │    40 │  9.35 ± 1.53 │
└──────────────────────────┴────────┴───────┴──────────────┘
6754.79s user 756.02s system 381% cpu 32:50.44 total
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
