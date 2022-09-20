## MIT 6.824 Distributed Systems: Lab 3 Fault-tolerant Key/Value Service 

Solution for [Lab 3: Fault-tolerant Key/Value Service](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)

Tests:
```sh
❯ time python3 mtests.py --timeout 1m --workers 4 -n 40 3A 3B
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                                                           ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestBasic3A                                                    │      0 │    40 │ 16.85 ± 0.50 │
│ TestSpeed3A                                                    │      0 │    40 │ 12.08 ± 1.05 │
│ TestConcurrent3A                                               │      0 │    40 │ 17.13 ± 0.52 │
│ TestUnreliable3A                                               │      0 │    40 │ 17.65 ± 0.57 │
│ TestUnreliableOneKey3A                                         │      0 │    40 │  2.65 ± 0.22 │
│ TestOnePartition3A                                             │      0 │    40 │  4.38 ± 0.22 │
│ TestManyPartitionsOneClient3A                                  │      0 │    40 │ 24.29 ± 0.25 │
│ TestManyPartitionsManyClients3A                                │      0 │    40 │ 24.77 ± 0.25 │
│ TestPersistOneClient3A                                         │      0 │    40 │ 23.41 ± 0.43 │
│ TestPersistConcurrent3A                                        │      0 │    40 │ 25.66 ± 0.70 │
│ TestPersistConcurrentUnreliable3A                              │      0 │    40 │ 26.10 ± 0.69 │
│ TestPersistPartition3A                                         │      0 │    40 │ 31.36 ± 0.81 │
│ TestPersistPartitionUnreliable3A                               │      0 │    40 │ 31.27 ± 0.67 │
│ TestPersistPartitionUnreliableLinearizable3A                   │      0 │    40 │ 34.35 ± 0.74 │
│ TestSnapshotRPC3B                                              │      0 │    40 │  3.62 ± 0.30 │
│ TestSnapshotSize3B                                             │      0 │    40 │  5.40 ± 0.45 │
│ TestSpeed3B                                                    │      0 │    40 │  7.01 ± 0.40 │
│ TestSnapshotRecover3B                                          │      0 │    40 │ 20.71 ± 0.33 │
│ TestSnapshotRecoverManyClients3B                               │      0 │    40 │ 22.34 ± 0.44 │
│ TestSnapshotUnreliable3B                                       │      0 │    40 │ 17.52 ± 0.31 │
│ TestSnapshotUnreliableRecover3B                                │      0 │    40 │ 21.74 ± 0.58 │
│ TestSnapshotUnreliableRecoverConcurrentPartition3B             │      0 │    40 │ 28.78 ± 0.39 │
│ TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B │      0 │    40 │ 30.31 ± 0.67 │
└────────────────────────────────────────────────────────────────┴────────┴───────┴──────────────┘
14426.18s user 2293.30s system 370% cpu 1:15:10.98 total
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
