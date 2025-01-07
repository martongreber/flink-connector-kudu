package org.apache.flink.connector.kudu.connector.reader;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.RowResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class KuduSplitReader implements SourceReader<String, KuduSplit> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final SourceReaderContext context;
    private final String kuduMasterAddresses; // Connection parameter
    private KuduClient kuduClient;            // KuduClient created and managed by the reader
    private KuduScanner scanner;
    private KuduSplit currentSplit;           // Track the current split being processed
    private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();
    private AtomicBoolean hasSignaledAvailability = new AtomicBoolean(false);

    public KuduSplitReader(SourceReaderContext context, String kuduMasterAddresses) {
        this.context = context;
        this.kuduMasterAddresses = kuduMasterAddresses;

        log.info("KuduSplitReader instantiated.");
    }

    @Override
    public void start() {
        // Create the KuduClient
        this.kuduClient = new KuduClient.KuduClientBuilder(kuduMasterAddresses).build();
        log.info("KuduSplitReader started.");
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
        log.info("pollNext() called");
        if (currentSplit == null) {
            log.info("pollNext currentSplit is null!");
            output.markIdle(); // Mark idle if no split is assigned
            resetAvailabilityFuture();
            return InputStatus.NOTHING_AVAILABLE;
        }

        String splitId = currentSplit.splitId();
        SourceOutput<String> splitOutput = output.createOutputForSplit(splitId);

        try {
            if (scanner != null) {
                if (scanner.hasMoreRows()) {
                    log.info("pollNext: scanner has some data!");
                    for (RowResult row : scanner.nextRows()) {
                        log.info("pollNext: " + row.toString());
                        splitOutput.collect(row.toString()); // Emit the row
                    }
                    if (scanner.hasMoreRows()) {
                        return InputStatus.MORE_AVAILABLE;
                    } else {
                        return InputStatus.END_OF_INPUT;
                    }
                } else {
                    log.info("no more data in the split");
                    output.releaseOutputForSplit(splitId);
                    currentSplit = null;
                    scanner.close();
                    resetAvailabilityFuture();
                    context.sendSplitRequest();
                    return InputStatus.END_OF_INPUT;
                }
            }
             else {
                log.info("scanner is null");
                return InputStatus.NOTHING_AVAILABLE;
            }
        } catch (Exception e) {
            log.info("pollNext exception");
            log.info(e.getMessage());
            output.releaseOutputForSplit(splitId);
            throw e;
        }
    }

    @Override
    public List<KuduSplit> snapshotState(long checkpointId) {
        return currentSplit != null ? Collections.singletonList(currentSplit) : Collections.emptyList();
    }


    @Override
    public CompletableFuture<Void> isAvailable() {
        return availabilityFuture;
    }

    private void resetAvailabilityFuture() {
        if (hasSignaledAvailability.compareAndSet(true, false)) {
            availabilityFuture = new CompletableFuture<>();
        }
    }

    private void completeAvailabilityFuture() {
        if (hasSignaledAvailability.compareAndSet(false, true)) {
            availabilityFuture.complete(null);
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        // Called when no more splits will be assigned to this reader
    }


    @Override
    public void addSplits(List<KuduSplit> splits) {
        if (splits.size() != 1) {
            throw new IllegalStateException("KuduSplitReader expects exactly one split at a time");
        }

        this.currentSplit = splits.get(0);

        byte[] serializedToken = currentSplit.getSerializedScanToken();
        try {
            this.scanner = KuduScanToken.deserializeIntoScanner(serializedToken, kuduClient);
            log.info("Deserialization into Kudu Scanner completed");


            // Signal availability for pollNext
            completeAvailabilityFuture();
        } catch (Exception e) {
            log.error("KuduSplit deserialization failed.", e);
            availabilityFuture.completeExceptionally(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (scanner != null) {
            scanner.close();
        }
        if (kuduClient != null) {
            kuduClient.close(); // Ensure KuduClient is cleaned up
        }
    }
}