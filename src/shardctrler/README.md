## MIT 6.824 Distributed Systems: Lab 4: Sharded Key/Value Service - Part A: The Shard controller

Solution for [Lab 4A: The Shard controller](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

Tests:
```sh
❯ time python3 mtests.py  --workers 4 -n 120 4A
┏━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━┓
┃ Test      ┃ Failed ┃ Total ┃        Time ┃
┡━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━┩
│ TestBasic │      0 │   120 │ 2.75 ± 0.36 │
│ TestMulti │      0 │   120 │ 1.78 ± 0.28 │
└───────────┴────────┴───────┴─────────────┘
324.95s user 104.59s system 312% cpu 2:17.35 total
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
