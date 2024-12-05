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
import org.apache.flink.api.connector.source.SourceSplit;

// Serialized byte[] is already compact and Flink-optimized, whereas the
// full KuduScanToken object might include unnecessary metadata or complex internal structures.

public class KuduSplit implements SourceSplit {
    private final String splitId;
    private final byte[] serializedScanToken;

    public KuduSplit(String splitId, byte[] serializedScanToken) {
        this.splitId = splitId;
        this.serializedScanToken = serializedScanToken;
    }

    public byte[] getSerializedScanToken() {
        return serializedScanToken;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
