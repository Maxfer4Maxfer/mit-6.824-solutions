## MIT 6.824 Distributed Systems: Lab 3 Fault-tolerant Key/Value Service 

Solution for [Lab 3: Fault-tolerant Key/Value Service](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)

Tests:
```sh
❯ time python3 mtests.py --timeout 1m --workers 4 -n 120 3A 3B
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                                                           ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestBasic3A                                                    │      0 │   120 │ 15.92 ± 0.19 │
│ TestSpeed3A                                                    │      0 │   120 │  6.79 ± 0.61 │
│ TestConcurrent3A                                               │      0 │   120 │ 16.26 ± 0.18 │
│ TestUnreliable3A                                               │      0 │   120 │ 16.61 ± 0.21 │
│ TestUnreliableOneKey3A                                         │      0 │   120 │  1.77 ± 0.19 │
│ TestOnePartition3A                                             │      0 │   120 │  3.63 ± 0.23 │
│ TestManyPartitionsOneClient3A                                  │      0 │   120 │ 23.63 ± 0.26 │
│ TestManyPartitionsManyClients3A                                │      0 │   120 │ 23.79 ± 0.32 │
│ TestPersistOneClient3A                                         │      0 │   120 │ 21.64 ± 0.37 │
│ TestPersistConcurrent3A                                        │      0 │   120 │ 23.72 ± 0.56 │
│ TestPersistConcurrentUnreliable3A                              │      0 │   120 │ 22.77 ± 0.35 │
│ TestPersistPartition3A                                         │      0 │   120 │ 29.52 ± 0.88 │
│ TestPersistPartitionUnreliable3A                               │      0 │   120 │ 28.84 ± 0.60 │
│ TestPersistPartitionUnreliableLinearizable3A                   │      0 │   120 │ 31.30 ± 0.63 │
│ TestSnapshotRPC3B                                              │      0 │   120 │  2.57 ± 0.21 │
│ TestSnapshotSize3B                                             │      0 │   120 │  2.45 ± 0.24 │
│ TestSpeed3B                                                    │      0 │   120 │  3.20 ± 0.41 │
│ TestSnapshotRecover3B                                          │      0 │   120 │ 19.97 ± 0.25 │
│ TestSnapshotRecoverManyClients3B                               │      0 │   120 │ 20.66 ± 0.43 │
│ TestSnapshotUnreliable3B                                       │      0 │   120 │ 16.81 ± 0.21 │
│ TestSnapshotUnreliableRecover3B                                │      0 │   120 │ 20.65 ± 0.34 │
│ TestSnapshotUnreliableRecoverConcurrentPartition3B             │      0 │   120 │ 27.81 ± 0.33 │
│ TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B │      0 │   120 │ 28.87 ± 0.41 │
└────────────────────────────────────────────────────────────────┴────────┴───────┴──────────────┘
34403.67s user 6760.87s system 334% cpu 3:25:00.79 total
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
