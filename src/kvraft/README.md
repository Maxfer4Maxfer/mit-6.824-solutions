## MIT 6.824 Distributed Systems: Lab 3 Fault-tolerant Key/Value Service 

Solution for [Lab 3: Fault-tolerant Key/Value Service](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)

Tests:
```sh
❯ time python3 mtests.py --timeout 2m --workers 4 -n 40 3A 3B
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                                                           ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestBasic3A                                                    │      0 │    40 │ 15.90 ± 0.12 │
│ TestSpeed3A                                                    │      0 │    40 │  6.78 ± 0.39 │
│ TestConcurrent3A                                               │      0 │    40 │ 16.35 ± 0.36 │
│ TestUnreliable3A                                               │      0 │    40 │ 16.58 ± 0.11 │
│ TestUnreliableOneKey3A                                         │      0 │    40 │  1.85 ± 0.21 │
│ TestOnePartition3A                                             │      0 │    40 │  3.62 ± 0.19 │
│ TestManyPartitionsOneClient3A                                  │      0 │    40 │ 23.61 ± 0.29 │
│ TestManyPartitionsManyClients3A                                │      0 │    40 │ 23.74 ± 0.33 │
│ TestPersistOneClient3A                                         │      0 │    40 │ 21.65 ± 0.37 │
│ TestPersistConcurrent3A                                        │      0 │    40 │ 23.79 ± 0.58 │
│ TestPersistConcurrentUnreliable3A                              │      0 │    40 │ 22.80 ± 0.30 │
│ TestPersistPartition3A                                         │      0 │    40 │ 29.44 ± 0.67 │
│ TestPersistPartitionUnreliable3A                               │      0 │    40 │ 28.80 ± 0.37 │
│ TestPersistPartitionUnreliableLinearizable3A                   │      0 │    40 │ 31.39 ± 0.56 │
│ TestSnapshotRPC3B                                              │      0 │    40 │  2.61 ± 0.22 │
│ TestSnapshotSize3B                                             │      0 │    40 │  2.52 ± 0.28 │
│ TestSpeed3B                                                    │      0 │    40 │  3.17 ± 0.21 │
│ TestSnapshotRecover3B                                          │      0 │    40 │ 20.03 ± 0.22 │
│ TestSnapshotRecoverManyClients3B                               │      0 │    40 │ 20.53 ± 0.22 │
│ TestSnapshotUnreliable3B                                       │      0 │    40 │ 16.85 ± 0.25 │
│ TestSnapshotUnreliableRecover3B                                │      0 │    40 │ 20.63 ± 0.34 │
│ TestSnapshotUnreliableRecoverConcurrentPartition3B             │      0 │    40 │ 27.82 ± 0.35 │
│ TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B │      0 │    40 │ 28.90 ± 0.33 │
└────────────────────────────────────────────────────────────────┴────────┴───────┴──────────────┘
11633.71s user 2154.41s system 335% cpu 1:08:30.44 total
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
