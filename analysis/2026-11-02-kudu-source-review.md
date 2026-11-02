<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Kudu source review: open issues (2026-11-02)

## Context

These notes come from reviewing the FLINK-38575 fix (branch `stall-fix`, commit `e5f19fd`). The
review checked the claims the fix relies on against the Flink 1.19.2 and master sources, attacked
the fix with tests (including a randomized model test of Flink's failover semantics), and looked
at the Kudu side and at the kudu-replication job (`~/kudu/java/kudu-replication`).

The FLINK-38575 fix itself held up. These notes record the issues the review found outside it, for
later analysis. Two findings are handled in the FLINK-38575 work and are not listed here:

- restoring a checkpoint written by connector 2.0.0, where a split may exist only in reader state;
- the test gaps found in the branch's own tests.

References:

- `main` = `3a4df68`. The source enumerator on `main` matches the released 2.0.0-1.19 jar.
- Flink file references are for `release-1.19.2` unless noted. Master behaves the same on every
  point below.
- Paths: `KSE` = `flink-connector-kudu/src/main/java/org/apache/flink/connector/kudu/source/enumerator/KuduSourceEnumerator.java`.

| # | Issue | Severity | Exists on |
|---|-------|----------|-----------|
| A | Split discovery changes enumerator state on a worker thread | High: data loss on restore, wrong ordering | `main`, 2.0.0, `stall-fix`, kudu-replication |
| B | BOUNDED mode signals "no more splits" before the initial discovery finished | High: data loss, job succeeds | `main`, 2.0.0, `stall-fix` |
| C | BATCH mode with speculative execution fails on the first `SplitFinishedEvent` | Medium: crash loop | `main`, 2.0.0, `stall-fix` |
| D | Minor behavior and docs points | Low | see section |
| E | Kudu-side and replication risks | Varies | see section |

## A. Split discovery changes enumerator state on a worker thread

### Mechanism

`SplitEnumeratorContext.callAsync(callable, handler, ...)` runs the **callable on a worker thread**
and only the **handler on the coordinator thread**. Every other enumerator method
(`handleSourceEvent`, `addSplitsBack`, `addReader`, `snapshotState`, `notifyCheckpointComplete`)
runs on the coordinator thread.

- Flink: `flink-runtime/.../source/coordinator/ExecutorNotifier.java`, `notifyReadyAsync`:
  `workerExecutor.execute(() -> { T result = callable.call(); executorToNotify.execute(() -> handler.accept(result, null)); })`,
  and the periodic variant with `workerExecutor.scheduleAtFixedRate(...)`.
  `SourceCoordinatorContext.callAsync` (lines 330-341) delegates to it.
- Connector: `start()` registers `enumerateNewSplits` as the callable and `assignSplits` as the
  handler (`KSE:132-135` on `main`).

`enumerateNewSplits()` (`KSE:194-210` on `main`) is not side-effect free:

- it reads `pending` and `unassigned` in `shouldEnumerateNewSplits()` (`KSE:254`);
- it writes `lastEndTimestamp` (`KSE:201` for the first range, `KSE:207` for later ranges).

The new splits reach `unassigned` only later, in the handler on the coordinator thread
(`KSE:220`).

### Consequences

1. **A time range is skipped after a global restore.** A checkpoint taken between the callable
   updating `lastEndTimestamp` and the handler adding the splits stores the new timestamp with no
   splits: `(lastEnd = t_new, unassigned = [], pending = [])`. A restore from it continues after
   `t_new`, and the range is never read.
   - For the first range, `lastEndTimestamp` is set **before** `generateFullScanSplits` builds the
     scan tokens (`KSE:201-202`). The window covers the whole token generation, which involves
     master RPCs, so the **entire initial snapshot** can be lost.
   - For later ranges the window is the time between the callable returning and the handler
     running on the coordinator thread.
   - In BOUNDED mode the same race turns the restore into a diff scan instead of the snapshot.
2. **Two time ranges at once.** If one periodic run is slow, or the coordinator thread is busy, the
   next run can start before the previous handler ran. The books still look empty, so the next
   range is generated too. Both ranges are then unassigned together and read in parallel, in any
   order. With the replication operation mapping (`UpsertIgnore` / `DeleteIgnore` with the
   end-of-range row image), an older range written after a newer one leaves stale, resurrected or
   wrongly deleted rows until the key changes again.
3. **Unsynchronized reads.** The worker thread reads the books without synchronization, so it can
   see them briefly empty while the coordinator thread moves a split between them. On `main` this
   happens in `assignSplitsToReaders` between `getNextSplit(unassigned)` and `pending.add`. On
   `stall-fix` it also happens in `handleSourceEvent` between `pending` and `pendingRemoval`, and
   in the subtask reset path. On `stall-fix` the worker iterates the `pending` `HashMap`, so a
   concurrent structural change can also throw `ConcurrentModificationException`. The handler then
   throws "Failed to enumerate Kudu splits", which fails the job globally.

The kudu-replication wrapper (`MetricWrappedKuduEnumerator`) delegates discovery unchanged, so the
replication job has the same issue. Its `handleSourceEvent` also briefly empties `pending` (the
delegate removes the split, the wrapper re-adds it), which widens the window in item 3.

`TestingSplitEnumeratorContext` runs the callable and the handler back to back
(`callableWithResultHandler`), so none of the existing tests can show this.

### Reproduction

Both tests fail. They were written against `stall-fix`, but the interleavings exist on `main` as
well. The test table has two tablets and 10 rows (`KuduSourceTestBase`).

```java
public class KuduSourceEnumeratorDiscoveryRaceTest extends KuduSourceTestBase {

    /** Lets the test run the callable (worker thread) and handler (coordinator thread) separately. */
    static class CapturingContext extends TestingSplitEnumeratorContext<KuduSourceSplit> {
        Callable<Object> callable;
        BiConsumer<Object, Throwable> handler;

        CapturingContext() {
            super(1);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> void callAsync(
                Callable<T> callable, BiConsumer<T, Throwable> handler, long delay, long period) {
            this.callable = (Callable<Object>) callable;
            this.handler = (BiConsumer<Object, Throwable>) handler;
        }

        Object runCallable() throws Exception {
            return callable.call();
        }

        void runHandler(Object result) {
            handler.accept(result, null);
        }
    }

    private static KuduSourceEnumerator create(
            CapturingContext ctx, KuduSourceEnumeratorState state) {
        KuduSourceEnumerator e =
                new KuduSourceEnumerator(
                        getTableInfo(),
                        getReaderConfig(),
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        Duration.ofSeconds(1),
                        ctx,
                        state);
        e.start();
        return e;
    }

    private static int countRows(List<KuduSourceSplit> splits) throws Exception {
        int rows = 0;
        try (KuduSourceSplitReader reader = new KuduSourceSplitReader(getReaderConfig())) {
            for (KuduSourceSplit split : splits) {
                reader.handleSplitsChanges(
                        new SplitsAddition<>(new ArrayList<>(Collections.singletonList(split))));
                RecordsWithSplitIds<RowResult> records = reader.fetch();
                while (records.nextSplit() != null) {
                    while (records.nextRecordFromSplit() != null) {
                        rows++;
                    }
                }
            }
        }
        return rows;
    }

    /** Consequence 1: the initial snapshot is in no checkpointed state. */
    @Test
    void testCheckpointBetweenDiscoveryCallableAndHandlerLosesTimeRange() throws Exception {
        assertThat(getTestRowsCount()).isEqualTo(10);
        KuduSourceEnumeratorState checkpoint;

        CapturingContext ctx = new CapturingContext();
        try (KuduSourceEnumerator enumerator = create(ctx, KuduSourceEnumeratorState.empty())) {
            Object fullScanSplits = ctx.runCallable(); // worker thread
            checkpoint = enumerator.snapshotState(1); // coordinator thread, before the handler
            ctx.runHandler(fullScanSplits); // coordinator thread
        }

        assertThat(checkpoint.getLastEndTimestamp()).isNotEqualTo(-1L);
        assertThat(checkpoint.getUnassigned()).isEmpty();
        assertThat(checkpoint.getPending()).isEmpty();

        // A global restore continues with a diff scan from t0; the 10 snapshot rows are never read.
        CapturingContext ctx2 = new CapturingContext();
        try (KuduSourceEnumerator restored = create(ctx2, checkpoint)) {
            ctx2.registerReader(0, "h");
            restored.addReader(0);
            ctx2.runHandler(ctx2.runCallable());
            List<KuduSourceSplit> splits =
                    new ArrayList<>(restored.snapshotState(2).getUnassigned());
            assertThat(countRows(splits)).isEqualTo(10); // fails: 0
        }
    }

    /** Consequence 2: two time ranges outstanding at the same time. */
    @Test
    void testTwoDiscoveryRunsBeforeHandlerYieldTwoTimeRangesAtOnce() throws Exception {
        CapturingContext ctx = new CapturingContext();
        try (KuduSourceEnumerator enumerator = create(ctx, KuduSourceEnumeratorState.empty())) {
            Object run1 = ctx.runCallable(); // full scan at t0
            Thread.sleep(5);
            Object run2 = ctx.runCallable(); // books still empty -> diff scan from t0
            ctx.runHandler(run1);
            ctx.runHandler(run2);

            // 2 tablets: 2 splits per time range
            assertThat(enumerator.snapshotState(1).getUnassigned()).hasSize(2); // fails: 4
        }
    }
}
```

### Proposed fix

Keep all enumerator state on the coordinator thread. The worker thread only does the expensive
token generation, from inputs captured on the coordinator thread, and returns its result:

```java
start():
    callAsync(() -> null, (ignored, error) -> maybeDiscover(), 0, interval)  // periodic tick only

maybeDiscover():  // coordinator thread
    if (!discovering && shouldEnumerateNewSplits()) {
        discovering = true;
        long from = lastEndTimestamp;
        callAsync(() -> generate(from), this::onDiscovered);  // one-shot
    }

generate(from):  // worker thread, no field reads or writes
    compute endHT and the splits for (from, endHT), return both

onDiscovered(result, error):  // coordinator thread
    discovering = false;
    lastEndTimestamp = result.end;
    unassigned.addAll(result.splits);
    assignSplitsToReaders();
```

A snapshot taken while discovery is in flight then still holds the old `lastEndTimestamp` with
empty books, and the same range is discovered again after a restore. Error handling must keep the
current behavior (fail the job) and reset `discovering`.

Tests need a context like `CapturingContext` above that lets the test interleave the callable, the
handler and `snapshotState`.

## B. BOUNDED mode signals "no more splits" before the initial discovery finished

### Mechanism

In BOUNDED mode `start()` schedules a single discovery with `callAsync` (`KSE:135`).
`assignSplitsToReaders()` signals `signalNoMoreSplits` to every waiting reader as soon as
`unassigned` is empty (`KSE:243-246`). It does not check whether the discovery has run yet.

A reader that registers and requests a split before the discovery finishes, for example because
the Kudu master is slow or the table has many tablets, gets "no more splits". It finishes without
reading anything. Splits discovered later go only to readers that are still waiting. The job ends
successfully with data missing.

Related, also on `main`: a BOUNDED job restored after all its splits finished (empty books,
`lastEndTimestamp != -1`) runs an incremental diff scan in `enumerateNewSplits`. A bounded job
should never read past its snapshot.

### Reproduction

Fails: the reader has received the "no more splits" signal. No Kudu cluster is needed because the
discovery never runs.

```java
@Test
void testRequestBeforeInitialDiscoveryGetsNoMoreSplits() throws Exception {
    TestingSplitEnumeratorContext<KuduSourceSplit> ctx = new TestingSplitEnumeratorContext<>(1);
    try (KuduSourceEnumerator e =
            new KuduSourceEnumerator(
                    KuduTableInfo.forTable("table"),
                    KuduReaderConfig.Builder.setMasters("master").build(),
                    Boundedness.BOUNDED,
                    Duration.ofSeconds(1),
                    ctx,
                    KuduSourceEnumeratorState.empty())) {
        e.start(); // discovery scheduled, not run yet
        ctx.registerReader(0, "h");
        e.addReader(0);
        e.handleSplitRequest(0, "h"); // KuduSourceReader.start()
        assertThat(ctx.getSplitAssignments().get(0).hasReceivedNoMoreSplitsSignal()).isFalse();
    }
}
```

### Proposed fix

- In BOUNDED mode, signal "no more splits" only after the discovery handler has run (a
  `discoveryDone` flag). The handler calls `assignSplitsToReaders()`, which then signals the
  readers still waiting if nothing is left.
- In BOUNDED mode, only discover when `lastEndTimestamp == -1`. A restore with a timestamp set
  means the snapshot was already generated.

This fits naturally with the fix for A, since both change how discovery completes.

## C. BATCH mode with speculative execution fails on the first `SplitFinishedEvent`

With batch speculative execution, `SourceCoordinator` requires the enumerator to implement
`SupportsHandleExecutionAttemptSourceEvent` for custom source events
(`SourceCoordinator.java:588-596` in 1.19.2). `KuduSourceEnumerator` does not implement it, so the
first `SplitFinishedEvent` throws `IllegalStateException`, which fails the job. The job then
restarts and fails again.

Not reproduced; found by reading the Flink source. To decide:

- implement `SupportsHandleExecutionAttemptSourceEvent`, taking care that with speculative
  attempts more than one attempt of a subtask can hold and finish the same split; or
- document that speculative execution is not supported.

## D. Minor points

1. **Unbounded mode stops whenever checkpoints cannot complete** (`stall-fix` design). With the
   FLINK-38575 fix a finished split is only removed once a checkpoint completes, so the source does
   not move on to the next range while checkpoints keep failing. Examples: persistent checkpoint
   failures within `execution.checkpointing.tolerable-failed-checkpoints`, or
   `execution.checkpointing.checkpoints-after-tasks-finish.enabled=false` while another task of the
   job has finished. Before the fix the source kept going. The docs warning on `stall-fix` only
   mentions disabled checkpointing; it should cover this too, and say that each range takes at
   least one checkpoint cycle.
2. **Stale entries in `readersAwaitingSplit`.** `assignSplitsToReaders` stops at the first
   registered reader that gets no split, so entries for other readers can survive a subtask reset.
   The new reader of that subtask then gets one extra outstanding split. Harmless (the multiplex
   reader handles several splits); the model test never produced a duplicate split in a reader.

## E. Kudu-side and replication risks

Found while checking the Kudu side. They are outside the enumerator fixes above, but several of
them make the failover paths more likely, or recovery impossible.

1. **Ancient history mark.** A diff scan with a start or end below the tablet history mark is
   rejected (`tablet_history_max_age_sec`, default 7 days; `~/kudu/src/kudu/tablet/tablet.cc:152`,
   `tablet_service.cc:2879-2920`). A job that is down or stalled for longer cannot resume from its
   checkpoint and needs a full resync.
2. **JobManager clock.** Snapshot timestamps come from the JobManager's wall clock
   (`KuduSourceUtils.getCurrentHybridTime`).
   - A clock ahead of the tablet servers by more than their max error fails scans ("Snapshot time
     ... in the future"). The resulting task failure is exactly the failover path the FLINK-38575
     fix deals with.
   - A clock that steps back, or a JobManager failover to a host whose clock is behind, makes
     `generateIncrementalSplits` throw on `start >= end`. The job then fails globally until the
     clock catches up.
3. **Full-scan splits are not fault tolerant.** `generateFullScanSplits` does not call
   `setFaultTolerant`, so a tablet server restart or leader change fails the task during the
   initial snapshot. Diff scans are fault tolerant (`diffScan()` forces it).
4. **A whole split is buffered in memory.** `KuduSourceSplitReader.fetch` reads the entire split
   into one batch before emitting anything. With the default `splitSizeBytes = -1` that is a whole
   tablet, which risks OOM and TaskManager death on large tables. TaskManager death is the case in
   which buffered sink writes are actually lost.
5. **Dropped range partitions are not replicated.** Dropping a range partition produces no delete
   deltas, so a diff scan never reports those rows and they remain at the destination.
6. **Diff-scan bounds are `[start, end)`** (`mvcc.h:52-82`, `memrowset.cc:554-560`). Consecutive
   ranges reuse the previous end as the next start, so there is no gap and no overlap. This is a
   fact to remember rather than an issue.
7. **Sink flushing on failure.** `KuduWriter.close()` flushes, and Flink calls it when a task fails
   or is cancelled. Buffered rows are therefore lost only if the TaskManager process dies, or for
   rows the sink itself rejected (row errors, timeouts). With `AUTO_FLUSH_BACKGROUND` the client
   can also reorder writes within a session (KUDU-1767). `writer.flushMode = AUTO_FLUSH_SYNC` would
   close both windows at a throughput cost.
8. **Replication schema coupling.** `CustomReplicationOperationMapper` writes columns by position,
   so source and sink schemas must stay identical; a source schema change makes the job fail
   repeatedly.

### kudu-replication wrapper (`MetricWrappedKuduEnumerator`) on a TaskManager failure

For reference until the wrapper is replaced by the connector fix. The job chains source and sink
(forward edge, same parallelism), so each subtask is its own failover region. Restarts are enabled
in production (`enableFailFast = false`, default exponential-delay restarts). On a task failure
Flink only calls `addSplitsBack` with the splits assigned after the last completed checkpoint C.
Readers are stateless, so for each split of a failed subtask:

| Split | Outcome |
|-------|---------|
| assigned after C | read again |
| assigned before C, still running | stays in `pending` forever: no new range is ever discovered, and the whole job stalls silently while it stays RUNNING. Visible through the `lastEndTimestamp` and `pendingCount` gauges. Heals on any global restore, as long as the range start is still above the history mark (E.1). |
| assigned before C, finished after C | not read again, and removed at the next `notifyCheckpointComplete`. Rows the sink still buffered are lost if the TaskManager died (E.7). |
| assigned after C, finished, then read again | its id is still in `splitsPendingRemoval`, so the next `notifyCheckpointComplete` removes it from `pending` while it is being re-read. A second failure then loses it, and the next range can start while it is still being read (ordering, A.2). |

`notifyCheckpointComplete` clears all of `splitsPendingRemoval`, including splits that finished
after that checkpoint's snapshot.

Replication test coverage: `TestReplicationCheckpoint` and `TestReplicationCheckpointStress` only
cancel the job and resubmit it from a checkpoint, which is a global restore, and a cancel also
flushes the sink. No test kills a TaskManager or fails a single task, and no test writes updates or
deletes, so `IS_DELETED` handling and stale or resurrected rows are untested end to end.

## Verified Flink behavior

Facts confirmed in the Flink sources (1.19.2, same on master unless noted) that the source design
relies on. Kept here so later analysis does not have to re-derive them.

1. The coordinator snapshot of checkpoint N is taken before barrier N is injected into the sources
   (`CheckpointCoordinator.startTriggeringCheckpoint`: coordinators at 676-711, tasks after 739).
2. Coordinator-to-task events sent after the coordinator snapshot are held back until the task
   acknowledges checkpoint N. Events sent before it must be delivered, or the checkpoint fails
   (`OperatorCoordinatorHolder:313-392`, `SubtaskGatewayImpl:139-143`).
3. Task-to-coordinator events are not aligned with checkpoints. An event sent before barrier N can
   be processed after the coordinator snapshot N. A failed send fails the task.
4. `SourceCoordinator.checkpointCoordinator` serializes the enumerator state synchronously on the
   coordinator thread. All `SplitEnumerator` callbacks run on that thread, **except the
   `callAsync` callables** (issue A). Enumerator creation runs on the JobManager main thread, and
   `close()` can run on a separate closing thread on global reset.
5. `SourceReaderBase` emits all records of a split before calling `onSplitFinished`
   (`SourceReaderBase:197-268`).
6. A task failure is never a global failover under the DefaultScheduler, whatever the failover
   strategy: the enumerator keeps running and `subtaskReset` is called per restarted subtask. The
   enumerator is rebuilt from a checkpoint only on a global failure (enumerator or coordinator
   exception, too many failed checkpoints, inconsistent stop-with-savepoint), on JobManager failover
   or on job submission from a checkpoint or savepoint. With the AdaptiveScheduler every failure is
   a full restore.
7. `SourceCoordinator.subtaskReset` always calls `addSplitsBack`, even with an empty list, although
   its javadoc only promises the call when splits were assigned after the last checkpoint.
   HybridSource forwards only non-empty lists, but forwards `addReader`.
8. Order on a subtask failure: `executionAttemptFailed` (reader unregistered), then `subtaskReset`
   / `addSplitsBack`, then deployment, then `addReader` on re-registration, then the first split
   request.
9. Events from a failed or cancelled attempt are rejected after it leaves RUNNING
   (`DefaultOperatorCoordinatorHandler:106-116`). Events already queued are processed before the
   reset.
10. A region restart restores the latest completed checkpoint and aborts pending checkpoints.
    Savepoints are not used for region restores. Exception: a synchronous savepoint
    (stop-with-savepoint) is notified but not stored. If a task fails after it, the region restarts
    from an older checkpoint until the forced global failover. Low risk with the Kudu sink.
11. `SourceOperator.open()` adds restored reader splits before `start()` (1.19.2: 358-370). On
    master this is skipped for `SupportsSplitReassignmentOnRecovery` sources.
12. `SinkWriterOperator.prepareSnapshotPreBarrier` calls `SinkWriter.flush(false)` before the
    barrier is forwarded, also for unaligned checkpoints. With AT_LEAST_ONCE or unaligned
    checkpoints, records emitted before barrier N are covered by checkpoint N, so these modes only
    add duplicates.
13. `notifyCheckpointComplete` is best effort. A lost or late notification only delays progress.
14. BATCH mode has no checkpoint coordinator: `notifyCheckpointComplete` is never called, and
    `subtaskReset(NO_CHECKPOINT)` passes every split ever assigned to the subtask. A restarted
    producer region also restarts its consumer regions, so re-reading those splits is consistent.
