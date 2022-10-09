## MIT 6.824 Distributed Systems: Lab 2 Raft

Solution for [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)

Tests:
```sh
❯ time python3 mtests.py  --timeout 2m --workers 4 -n 40 2A 2B 2C 2D
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                             ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestInitialElection2A            │      0 │    40 │  3.52 ± 0.13 │
│ TestReElection2A                 │      0 │    40 │  4.98 ± 0.14 │
│ TestManyElections2A              │      0 │    40 │  5.96 ± 0.15 │
│ TestBasicAgree2B                 │      0 │    40 │  1.00 ± 0.13 │
│ TestRPCBytes2B                   │      0 │    40 │  1.86 ± 0.05 │
│ TestFor2023TestFollowerFailure2B │      0 │    40 │  4.94 ± 0.09 │
│ TestFor2023TestLeaderFailure2B   │      0 │    40 │  5.16 ± 0.11 │
│ TestFailAgree2B                  │      0 │    40 │  5.35 ± 0.78 │
│ TestFailNoAgree2B                │      0 │    40 │  3.81 ± 0.08 │
│ TestConcurrentStarts2B           │      0 │    40 │  1.05 ± 0.09 │
│ TestRejoin2B                     │      0 │    40 │  4.98 ± 0.94 │
│ TestBackup2B                     │      0 │    40 │ 17.99 ± 0.13 │
│ TestCount2B                      │      0 │    40 │  2.57 ± 0.09 │
│ TestPersist12C                   │      0 │    40 │  3.51 ± 0.10 │
│ TestPersist22                    │      0 │    40 │ 15.51 ± 0.92 │
│ TestPersist32C                   │      0 │    40 │  1.82 ± 0.10 │
│ TestFigure82C                    │      0 │    40 │ 31.96 ± 2.45 │
│ TestUnreliableAgree2C            │      0 │    40 │  1.84 ± 0.16 │
│ TestFigure8Unreliable2C          │      0 │    40 │ 34.60 ± 2.37 │
│ TestReliableChurn2C              │      0 │    40 │ 16.82 ± 0.18 │
│ TestUnreliableChurn2C            │      0 │    40 │ 16.81 ± 0.15 │
│ TestSnapshotBasic2D              │      0 │    40 │  4.60 ± 0.09 │
│ TestSnapshotInstall2D            │      0 │    40 │ 41.61 ± 2.82 │
│ TestSnapshotInstallUnreliable2D  │      0 │    40 │ 54.97 ± 3.88 │
│ TestSnapshotInstallCrash2D       │      0 │    40 │ 26.96 ± 0.28 │
│ TestSnapshotInstallUnCrash2D     │      0 │    40 │ 29.23 ± 0.62 │
│ TestSnapshotAllCrash2D           │      0 │    40 │  7.85 ± 0.76 │
└──────────────────────────────────┴────────┴───────┴──────────────┘
2702.19s user 697.95s system 96% cpu 58:50.57 total
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
