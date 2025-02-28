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

package org.apache.flink.connector.kudu.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.config.BoundednessSettings;
import org.apache.flink.connector.kudu.source.config.BoundednessSettingsBuilder;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link KuduSourceEnumerator}.
 *
 * <p>Verifies that when a registered reader requests a split:
 *
 * <ul>
 *   <li>An unassigned split moves to the enumerator context's split assignment.
 *   <li>The enumerator tracks the split by adding it to the pending list.
 * </ul>
 */
public class KuduSourceEnumeratorTest {

    private TestingSplitEnumeratorContext<KuduSourceSplit> context;
    private KuduSourceSplit split;
    private KuduSourceEnumerator enumerator;
    private final int subtaskId = 1;
    private final long checkpointId = 1L;
    private final String requesterHostname = "host";

    @BeforeEach
    void setup() {
        this.context = new TestingSplitEnumeratorContext<>(1);
    }

    private KuduSourceEnumerator createEnumerator(
            SplitEnumeratorContext<KuduSourceSplit> context, Boundedness boundedness) {
        KuduTableInfo tableInfo = KuduTableInfo.forTable("table");
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters("master").build();
        BoundednessSettings boundednessSettings =
                new BoundednessSettingsBuilder()
                        .setDiscoveryInterval(Duration.ofSeconds(1))
                        .setBoundedness(boundedness)
                        .build();

        byte[] token = {1, 2, 3, 4, 5};
        split = new KuduSourceSplit(token);

        List<KuduSourceSplit> unassigned = new ArrayList<>();
        unassigned.add(split);
        List<KuduSourceSplit> pending = new ArrayList<>();

        KuduSourceEnumeratorState state = new KuduSourceEnumeratorState(1L, unassigned, pending);

        return new KuduSourceEnumerator(
                tableInfo, readerConfig, boundednessSettings, context, state);
    }

    @ParameterizedTest
    @EnumSource(Boundedness.class)
    void testCheckpointNoSplitRequested(Boundedness boundedness) throws Exception {
        enumerator = createEnumerator(context, boundedness);
        KuduSourceEnumeratorState state = enumerator.snapshotState(checkpointId);
        assertThat(state.getUnassigned().size()).isEqualTo(1);
        assertThat(state.getPending().size()).isEqualTo(0);
    }

    @ParameterizedTest
    @EnumSource(Boundedness.class)
    void testSplitRequestForRegisteredReader(Boundedness boundedness) throws Exception {
        enumerator = createEnumerator(context, boundedness);
        context.registerReader(subtaskId, requesterHostname);
        enumerator.addReader(subtaskId);
        enumerator.handleSplitRequest(subtaskId, requesterHostname);
        assertThat(enumerator.snapshotState(checkpointId).getUnassigned().size()).isEqualTo(0);
        assertThat(enumerator.snapshotState(checkpointId).getPending().size()).isEqualTo(1);
        assertThat(context.getSplitAssignments().size()).isEqualTo(1);
        assertThat(context.getSplitAssignments().get(subtaskId).getAssignedSplits())
                .contains(split);
    }

    @ParameterizedTest
    @EnumSource(Boundedness.class)
    void testSplitRequestForNonRegisteredReader(Boundedness boundedness) throws Exception {
        enumerator = createEnumerator(context, boundedness);
        enumerator.handleSplitRequest(subtaskId, requesterHostname);
        assertThat(context.getSplitAssignments().size()).isEqualTo(0);
        assertThat(enumerator.snapshotState(checkpointId).getUnassigned().size()).isEqualTo(1);
        assertThat(enumerator.snapshotState(checkpointId).getUnassigned().get(0)).isEqualTo(split);
        assertThat(enumerator.snapshotState(checkpointId).getPending().size()).isEqualTo(0);
    }
}
