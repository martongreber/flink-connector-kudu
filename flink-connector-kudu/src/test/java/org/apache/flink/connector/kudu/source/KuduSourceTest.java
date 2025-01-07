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

package org.apache.flink.connector.kudu.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.KuduTestBase;
import org.apache.flink.connector.kudu.connector.source.KuduSource;
import org.apache.flink.connector.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connector.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connector.kudu.sink.KuduSink;
import org.apache.flink.connector.kudu.sink.KuduSinkBuilder;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.platform.commons.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KuduSourceTest extends KuduTestBase {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Logger kudu_log = LoggerFactory.getLogger("kudu log");
    private static final String TEST_TABLE_NAME = UUID.randomUUID().toString();

    private void setUpTable() throws Exception {
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(kudu_log);
        master.followOutput(logConsumer);
        for(GenericContainer<?> tserver : tServers) {
            tserver.followOutput(logConsumer);
        }

        KuduClient client =  new KuduClient.KuduClientBuilder(getMasterAddress()).build();

        // Set up a simple schema.
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                .nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("key");
        int numBuckets = 2;
        cto.addHashPartitions(hashKeys, numBuckets);
        cto.setNumReplicas(1);

        // Create the table if it doesn't already exist.
        try {
            client.createTable(TEST_TABLE_NAME, schema, cto);
        } catch (KuduException e) {
            if (!e.getStatus().isAlreadyPresent()) {
                throw e; // Re-throw if the error is not about the table already existing
            }
        }

        try {
            insertTestData(client, TEST_TABLE_NAME, 10);
        } catch (KuduException e) {}
    }

    private void insertTestData(KuduClient client, String tableName, int numRows) throws KuduException {
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

        try {
            for (int i = 0; i < numRows; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt("key", i); // Primary key
                row.addString("value", "value-" + i); // Some string value
                session.apply(insert);
            }
            session.flush(); // Flush all pending writes
            // Check for errors
            RowErrorsAndOverflowStatus errors = session.getPendingErrors();
            if (errors.isOverflowed()) {
                System.err.println("Some insert errors were dropped due to overflow");
            }
            for (RowError error : errors.getRowErrors()) {
                System.err.println("Insert error: " + error.toString());
            }
        } finally {
            session.close();
            client.close();
        }

    }

    private RowOperationMapper initOperationMapper(String[] cols) {
        return new RowOperationMapper(cols, AbstractSingleOperationMapper.KuduOperation.INSERT);
    }

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(2)
                                .setNumberTaskManagers(1)
                                .build());

        @Test
        public void testKuduSourceReadsData() throws Exception {
            try{
                setUpTable();
            } catch (Exception e) {
                log.error(e.getMessage());
            }

//            KuduTableInfo tableInfo = booksTableInfo(TEST_TABLE_NAME, true);
//
//            setUpDatabase(tableInfo);

            // Set up Flink environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Use KuduSource
            KuduSource kuduSource = new KuduSource(this.getMasterAddress(), TEST_TABLE_NAME);
            DataStream<String> stream = env.fromSource(kuduSource,WatermarkStrategy.noWatermarks(),
                                            "Kudu Source Test");
            stream.print();
            env.execute("Kudu Test");
    }

}

