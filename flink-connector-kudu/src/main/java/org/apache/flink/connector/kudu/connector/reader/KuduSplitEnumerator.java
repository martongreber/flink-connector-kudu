/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kudu.connector.reader;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.kudu.client.*;
import org.apache.kudu.util.HybridTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class KuduSplitEnumerator implements SplitEnumerator<KuduSplit, KuduSourceEnumState> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final SplitEnumeratorContext<KuduSplit> context;
    private final String kuduMasterAddresses;
    private KuduClient kuduClient;
    private final String kuduTableName;
    private KuduTable kuduTable;
    private final List<KuduSplit> pendingSplits;
    private long lastStopTimestamp; // Tracks the global scan state
    private volatile boolean running = true;
    private boolean isFirstScan = true; // Tracks whether to perform the initial full scan

    public KuduSplitEnumerator(
            SplitEnumeratorContext<KuduSplit> context,
            String kuduMasterAddresses,
            String kuduTableName
    ) {
        this.context = context;
        this.kuduTableName = kuduTableName;
        this.pendingSplits = new ArrayList<>();
        this.kuduMasterAddresses = kuduMasterAddresses;

        log.info("KuduSplitEnumerator initialized.");
    }

    // Helper method to get the current Hybrid Time
    private long getCurrentHybridTime() {
        long fromMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        return HybridTimeUtil.physicalAndLogicalToHTTimestamp(fromMicros, 0) + 1;
    }

    @VisibleForTesting
    public List<KuduSplit> getPendingSplits() {
        return this.pendingSplits;
    }

    @Override
    public void start() {

        this.kuduClient = new KuduClient.KuduClientBuilder(this.kuduMasterAddresses).build();
        try {
            this.kuduTable = this.kuduClient.openTable(this.kuduTableName);
        } catch (KuduException e) {
            log.info("Failed to open Kudu table: {}", e.getMessage());
        }
        this.lastStopTimestamp = getCurrentHybridTime();

        new Thread(() -> {
            try {
                while (running) {
                    generateSplits();
                    Thread.sleep(TimeUnit.MINUTES.toMillis(2));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        synchronized (pendingSplits) {
            if (!pendingSplits.isEmpty()) {
                // Assign a pending split to the requesting reader
                KuduSplit split = pendingSplits.remove(0);
                context.assignSplit(split, subtaskId);
            }
        }
    }

    @Override
    public void addSplitsBack(List<KuduSplit> splits, int subtaskId) {
        synchronized (pendingSplits) {
            // Re-add unprocessed splits to the pending list
            pendingSplits.addAll(splits);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // Optional: You can log or handle new readers joining the enumerator
    }

    @Override
    public KuduSourceEnumState snapshotState(long checkpointId) {
        // Persist the current state for fault tolerance
        return new KuduSourceEnumState(lastStopTimestamp);
    }

    @Override
    public void close() {
        // Clean up resources (e.g., close the Kudu client if necessary)
        try {
            kuduClient.close(); // Ensure KuduClient is closed
        } catch (Exception e) {
            throw new RuntimeException("Error closing KuduClient", e);
        }
        running = false;
    }

    private void generateSplits() {
        if (isFirstScan) {
            performFullScan(); // Initial full scan
            isFirstScan = false; // Mark the full scan as complete
        } else {
            performIncrementalScan(); // Incremental diff scan
        }
    }

    private void performFullScan() {
        try {
            log.info("Performing full scan.");
            List<KuduScanToken> tokens = kuduClient.newScanTokenBuilder(kuduTable)
                    .snapshotTimestampRaw(lastStopTimestamp) // Use Hybrid Time as the snapshot timestamp
                    .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)
                    .build();

            List<KuduSplit> newSplits = new ArrayList<>();
            for (KuduScanToken token : tokens) {
                byte[] serializedToken = token.serialize();
                String splitId = UUID.randomUUID().toString();
                newSplits.add(new KuduSplit(splitId, serializedToken));
            }

            synchronized (pendingSplits) {
                pendingSplits.addAll(newSplits);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error during full snapshot scan", e);
        }
    }

    private void performIncrementalScan() {
        long startHT = lastStopTimestamp; // Use the last stop timestamp as the start HT
        long endMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        long endHT = HybridTimeUtil.physicalAndLogicalToHTTimestamp(endMicros, 0) + 1;

        try {
            List<KuduScanToken> tokens = kuduClient.newScanTokenBuilder(kuduTable)
                    .diffScan(startHT, endHT) // Use Hybrid Time for incremental range
                    .build();

            List<KuduSplit> newSplits = new ArrayList<>();
            for (KuduScanToken token : tokens) {
                try {
                    byte[] serializedToken = token.serialize();
                    String splitId = UUID.randomUUID().toString();
                    newSplits.add(new KuduSplit(splitId, serializedToken));
                } catch (Exception e) {
                    // Handle serialization failure
                }
            }

            synchronized (pendingSplits) {
                pendingSplits.addAll(newSplits);
            }

            lastStopTimestamp = endHT; // Update the last stop timestamp
        } catch (Exception e) {
            throw new RuntimeException("Error during incremental diff scan", e);
        }
    }
}