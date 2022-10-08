## MIT 6.824 Distributed Systems: Lab 4: Sharded Key/Value Service - Part A: The Shard controller

Solution for [Lab 4A: The Shard controller](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

Tests:
```sh
❯ time python3 mtests.py  --workers 4 -n 40 4A
┏━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━┓
┃ Test      ┃ Failed ┃ Total ┃        Time ┃
┡━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━┩
│ TestBasic │      0 │    40 │ 2.83 ± 0.50 │
│ TestMulti │      0 │    40 │ 1.85 ± 0.40 │
└───────────┴────────┴───────┴─────────────┘
112.74s user 37.34s system 310% cpu 48.391 total
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
