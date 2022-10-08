## MIT 6.824 Distributed Systems: Lab 2 Raft

Solution for [Lab 2: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)

Tests:
```sh
❯ time python3 mtests.py  --timeout 1m --workers 4 -n 40 2A 2B 2C 2D
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Test                             ┃ Failed ┃ Total ┃         Time ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ TestInitialElection2A            │      0 │    40 │  3.77 ± 0.35 │
│ TestReElection2A                 │      0 │    40 │  5.22 ± 0.35 │
│ TestManyElections2A              │      0 │    40 │  6.23 ± 0.37 │
│ TestBasicAgree2B                 │      0 │    40 │  1.27 ± 0.37 │
│ TestRPCBytes2B                   │      0 │    40 │  2.09 ± 0.13 │
│ TestFor2023TestFollowerFailure2B │      0 │    40 │  5.19 ± 0.12 │
│ TestFor2023TestLeaderFailure2B   │      0 │    40 │  5.43 ± 0.12 │
│ TestFailAgree2B                  │      0 │    40 │  4.38 ± 0.40 │
│ TestFailNoAgree2B                │      0 │    40 │  4.02 ± 0.10 │
│ TestConcurrentStarts2B           │      0 │    40 │  1.36 ± 0.23 │
│ TestRejoin2B                     │      0 │    40 │  5.80 ± 1.08 │
│ TestBackup2B                     │      0 │    40 │ 17.57 ± 0.95 │
│ TestCount2B                      │      0 │    40 │  2.97 ± 0.23 │
│ TestPersist12C                   │      0 │    40 │  3.81 ± 0.22 │
│ TestPersist22                    │      0 │    40 │ 15.21 ± 0.62 │
│ TestPersist32C                   │      0 │    40 │  2.01 ± 0.11 │
│ TestFigure82C                    │      0 │    40 │ 33.05 ± 2.62 │
│ TestUnreliableAgree2C            │      0 │    40 │  2.09 ± 0.11 │
│ TestFigure8Unreliable2C          │      0 │    40 │ 34.95 ± 2.29 │
│ TestReliableChurn2C              │      0 │    40 │ 17.03 ± 0.19 │
│ TestUnreliableChurn2C            │      0 │    40 │ 17.12 ± 0.15 │
│ TestSnapshotBasic2D              │      0 │    40 │  4.97 ± 0.21 │
│ TestSnapshotInstall2D            │      0 │    40 │ 38.20 ± 1.44 │
│ TestSnapshotInstallUnreliable2D  │      0 │    40 │ 43.63 ± 2.95 │
│ TestSnapshotInstallCrash2D       │      0 │    40 │ 27.21 ± 0.24 │
│ TestSnapshotInstallUnCrash2D     │      0 │    40 │ 29.34 ± 0.77 │
│ TestSnapshotAllCrash2D           │      0 │    40 │  8.39 ± 0.80 │
└──────────────────────────────────┴────────┴───────┴──────────────┘
3778.06s user 1041.72s system 140% cpu 57:07.18 total
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
