## MIT 6.824 Distributed Systems: Lab 2 Raft

Solution for [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)

Tests:
```sh
❯ time python3 mtests.py  --timeout 2m --workers 4 -n 120 2A 2B 2C 2D
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                             ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestInitialElection2A            │      0 │   120 │  3.57 ± 0.15 │
│ TestReElection2A                 │      0 │   120 │  5.05 ± 0.19 │
│ TestManyElections2A              │      0 │   120 │  6.03 ± 0.20 │
│ TestBasicAgree2B                 │      0 │   120 │  1.07 ± 0.17 │
│ TestRPCBytes2B                   │      0 │   120 │  1.94 ± 0.14 │
│ TestFor2023TestFollowerFailure2B │      0 │   120 │  5.03 ± 0.15 │
│ TestFor2023TestLeaderFailure2B   │      0 │   120 │  5.27 ± 0.17 │
│ TestFailAgree2B                  │      0 │   120 │  5.50 ± 0.75 │
│ TestFailNoAgree2B                │      0 │   120 │  3.89 ± 0.12 │
│ TestConcurrentStarts2B           │      0 │   120 │  1.11 ± 0.15 │
│ TestRejoin2B                     │      0 │   120 │  5.30 ± 1.02 │
│ TestBackup2B                     │      0 │   120 │ 17.94 ± 0.57 │
│ TestCount2B                      │      0 │   120 │  2.66 ± 0.16 │
│ TestPersist12C                   │      0 │   120 │  3.59 ± 0.25 │
│ TestPersist22                    │      0 │   120 │ 15.33 ± 0.72 │
│ TestPersist32C                   │      0 │   120 │  1.90 ± 0.16 │
│ TestFigure82C                    │      0 │   120 │ 31.99 ± 2.54 │
│ TestUnreliableAgree2C            │      0 │   120 │  1.90 ± 0.20 │
│ TestFigure8Unreliable2C          │      0 │   120 │ 34.43 ± 2.75 │
│ TestReliableChurn2C              │      0 │   120 │ 16.89 ± 0.24 │
│ TestUnreliableChurn2C            │      0 │   120 │ 16.95 ± 0.25 │
│ TestSnapshotBasic2D              │      0 │   120 │  4.71 ± 0.19 │
│ TestSnapshotInstall2D            │      0 │   120 │ 42.19 ± 3.25 │
│ TestSnapshotInstallUnreliable2D  │      0 │   120 │ 54.13 ± 5.17 │
│ TestSnapshotInstallCrash2D       │      0 │   120 │ 27.00 ± 0.32 │
│ TestSnapshotInstallUnCrash2D     │      0 │   120 │ 29.46 ± 0.72 │
│ TestSnapshotAllCrash2D           │      0 │   120 │  8.16 ± 0.71 │
└──────────────────────────────────┴────────┴───────┴──────────────┘
8990.09s user 2311.84s system 106% cpu 2:56:43.65 total
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
