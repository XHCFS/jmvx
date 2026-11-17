# JMVX: sun.misc.Unsafe monitors vs JNI monitors (record/replay)

Task: in JMVX's record/replay mode, replace the `sun.misc.Unsafe` `monitorEnter`/`monitorExit`
calls with equivalent JNI code and measure the overhead.

Base: bitslab/jmvx branch `rto`, commit ae449f9.

## What changed
- `JMVXMonitor.java` and `native/jmvxmon.c`: JNI `MonitorEnter`/`MonitorExit`, loaded by absolute
  path through `-Djmvx.monitorlib` (boot-classpath classes do not search `java.library.path`).
- Every `unsafe.monitorEnter`/`monitorExit` call in `Recorder`, `Replayer`,
  `RecorderRTOArtifactTracer` and `JMVXRuntime` now calls `JMVXMonitor.enter`/`exit`.
- Two patches that were needed to get replay running on OpenJDK 8u512, applied to BOTH versions:
  - `getEntry` interception: removed the `addSlash` argument, because this JDK's native is
    `getEntry(long, byte[])` and the three-argument form never matched.
  - `getManifestNum`: now recorded and replayed like `getTotal`. Before, replay called the
    native on a stale pointer and crashed with SIGSEGV.

## Method
- Machine: Arch Linux, 12 logical cores, 15 GB RAM, OpenJDK 8u512, no CPU pinning, other
  programs were not controlled for.
- Benchmarks: DaCapo 23.11-chopin `batik` (`-t 4`, config `recorder.properties` and
  `replayer.properties`), and DaCapo 9.12 `avrora` (record only).
- 20 pairs per benchmark. Within a pair the order alternates (Unsafe first on odd runs, JNI first
  on even runs). Each version records and then replays its own log. Time is the harness
  `PASSED in N msec` value.
- Scripts: `run_rr.sh` (batik), `run_pairs.sh` (avrora). Analysis: `analyze.py` (prints the tables).
  Raw data: `batik_rr.csv`, `avrora_record.csv`.

## Result
No overhead could be measured.

| benchmark / phase | Unsafe median | JNI median | median diff | 95% CI |
|---|---|---|---|---|
| avrora record | 3996 ms | 3990 ms | -0.15% | -0.5 to +0.5 |
| batik record | 6132 ms | 6062 ms | -1.2% | -11.1 to +3.3 |
| batik replay | 6210 ms | 6253 ms | +0.7% | -4.6 to +11.5 |

All p values (Welch, paired t, Mann-Whitney, Wilcoxon) are above 0.25.

## Caveats
- 4 of 20 batik pairs had a run of 12 to 16 s against about 6 s typical, in both versions, so
  means are unreliable and medians with bootstrap intervals are reported. Cause not investigated.
- The batik intervals are about +/-10%, so a small overhead is not ruled out. batik probably
  does not take locks heavily. A lock-heavy benchmark was not tested.
- Only one JVM (OpenJDK 8u512) was tried. The repo's Dockerfile expects Oracle JDK 8u231.
