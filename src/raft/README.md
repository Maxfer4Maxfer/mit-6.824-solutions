## MIT 6.824 Distributed Systems: Lab 2 Raft

Solution for [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)

Tests:
```sh
❯ time python3 mtests.py  --timeout 1m --workers 4 -n 40 2A 2B 2C 2D
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                             ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestInitialElection2A            │      0 │    40 │  3.76 ± 0.19 │
│ TestReElection2A                 │      0 │    40 │  5.25 ± 0.22 │
│ TestManyElections2A              │      0 │    40 │  6.24 ± 0.22 │
│ TestBasicAgree2B                 │      0 │    40 │  1.24 ± 0.18 │
│ TestRPCBytes2B                   │      0 │    40 │  2.08 ± 0.09 │
│ TestFor2023TestFollowerFailure2B │      0 │    40 │  5.18 ± 0.13 │
│ TestFor2023TestLeaderFailure2B   │      0 │    40 │  5.41 ± 0.16 │
│ TestFailAgree2B                  │      0 │    40 │  4.35 ± 0.29 │
│ TestFailNoAgree2B                │      0 │    40 │  4.01 ± 0.09 │
│ TestConcurrentStarts2B           │      0 │    40 │  1.28 ± 0.12 │
│ TestRejoin2B                     │      0 │    40 │  5.94 ± 0.95 │
│ TestBackup2B                     │      0 │    40 │ 17.60 ± 0.92 │
│ TestCount2B                      │      0 │    40 │  3.04 ± 0.23 │
│ TestPersist12C                   │      0 │    40 │  3.88 ± 0.23 │
│ TestPersist22                    │      0 │    40 │ 15.00 ± 0.80 │
│ TestPersist32C                   │      0 │    40 │  2.03 ± 0.13 │
│ TestFigure82C                    │      0 │    40 │ 32.15 ± 2.90 │
│ TestUnreliableAgree2C            │      0 │    40 │  2.10 ± 0.17 │
│ TestFigure8Unreliable2C          │      0 │    40 │ 34.36 ± 3.52 │
│ TestReliableChurn2C              │      0 │    40 │ 17.07 ± 0.27 │
│ TestUnreliableChurn2C            │      0 │    40 │ 17.15 ± 0.20 │
│ TestSnapshotBasic2D              │      0 │    40 │  4.95 ± 0.17 │
│ TestSnapshotInstall2D            │      0 │    40 │ 38.03 ± 1.25 │
│ TestSnapshotInstallUnreliable2D  │      0 │    40 │ 43.47 ± 2.82 │
│ TestSnapshotInstallCrash2D       │      0 │    40 │ 27.22 ± 0.28 │
│ TestSnapshotInstallUnCrash2D     │      0 │    40 │ 29.69 ± 0.85 │
│ TestSnapshotAllCrash2D           │      0 │    40 │  8.25 ± 0.73 │
└──────────────────────────────────┴────────┴───────┴──────────────┘
4227.86s user 1097.74s system 155% cpu 56:55.30 total
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