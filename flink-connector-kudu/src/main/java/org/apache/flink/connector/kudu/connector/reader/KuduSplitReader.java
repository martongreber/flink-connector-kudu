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

public class KuduSplitReader implements SourceReader<String, KuduSplit> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final SourceReaderContext context;
    private final String kuduMasterAddresses; // Connection parameter
    private KuduClient kuduClient;            // KuduClient created and managed by the reader
    private KuduScanner scanner;
    private KuduSplit currentSplit;           // Track the current split being processed

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
    }

    @Override
    public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
        if (currentSplit == null) {
            output.markIdle(); // Mark idle if no split is assigned
            return InputStatus.END_OF_INPUT;
        }

        String splitId = currentSplit.splitId();
        SourceOutput<String> splitOutput = output.createOutputForSplit(splitId);

        try {
            if (scanner != null && scanner.hasMoreRows()) {
                for (RowResult row : scanner.nextRows()) {
                    splitOutput.collect(row.toString()); // Emit the row
                }
                return InputStatus.MORE_AVAILABLE; // Indicate more data to process
            } else {
                // No more rows, release the split output
                output.releaseOutputForSplit(splitId);
                currentSplit = null; // Mark the split as processed
                return InputStatus.END_OF_INPUT;
            }
        } catch (Exception e) {
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
        return CompletableFuture.completedFuture(null); // Indicate the reader is always ready
    }

    @Override
    public void notifyNoMoreSplits() {
        // Called when no more splits will be assigned to this reader
    }

    @Override
    public void addSplits(List<KuduSplit> splits) {
        if (splits.size() != 1) {
//            throw new IllegalStateException("KuduSplitReader expects exactly one split at a time");
        }

        this.currentSplit = splits.get(0);

        // Deserialize the scan token to create a Kudu scanner
        byte[] serializedToken = currentSplit.getSerializedScanToken();
        try {
            this.scanner = KuduScanToken.deserializeIntoScanner(serializedToken, kuduClient);
        } catch (Exception e) {
            log.info("KuduSplit deserialization failed.", e);
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