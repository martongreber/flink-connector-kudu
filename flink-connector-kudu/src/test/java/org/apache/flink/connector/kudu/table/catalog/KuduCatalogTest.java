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

package org.apache.flink.connector.kudu.table.catalog;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kudu.connector.KuduTestBase;
import org.apache.flink.connector.kudu.table.KuduTableTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link KuduCatalog}. */
public class KuduCatalogTest extends KuduTestBase {

    private KuduCatalog catalog;
    private StreamTableEnvironment tableEnv;

    @BeforeEach
    public void init() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        catalog = new KuduCatalog(getMasterAddress());
        tableEnv = KuduTableTestUtils.createTableEnvInStreamingMode(env);
        tableEnv.registerCatalog("kudu", catalog);
        tableEnv.useCatalog("kudu");
    }

    @Test
    public void testCreateAlterDrop() throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE TestTable1 (`first` STRING PRIMARY KEY NOT ENFORCED, `second` String)");
        tableEnv.executeSql("INSERT INTO TestTable1 VALUES ('f', 's')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        // Add this once Primary key support has been enabled
        // tableEnv.sqlUpdate("CREATE TABLE TestTable2 (`first` STRING, `second` String, PRIMARY
        // KEY(`first`)) WITH ('hash-columns' = 'first')");
        // tableEnv.sqlUpdate("INSERT INTO TestTable2 VALUES ('f', 's')");

        validateSingleKey("TestTable1");
        // validateSingleKey("TestTable2");

        tableEnv.executeSql("ALTER TABLE TestTable1 RENAME TO TestTable1R");
        validateSingleKey("TestTable1R");

        tableEnv.executeSql("DROP TABLE TestTable1R");
        assertFalse(getClient().tableExists("TestTable1R"));
    }

    @Test
    public void testCreateAndInsertMultiKey() throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE TestTable3 (`first` STRING, `second` INT, third STRING, PRIMARY KEY (`first`, `second`) NOT ENFORCED)");
        tableEnv.executeSql("INSERT INTO TestTable3 VALUES ('f', 2, 't')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        validateMultiKey("TestTable3");
    }

    @Test
    public void testSourceProjection() throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE TestTable5 (`second` String PRIMARY KEY NOT ENFORCED, `first` STRING, `third` String)");
        tableEnv.executeSql("INSERT INTO TestTable5 VALUES ('s', 'f', 't')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        tableEnv.executeSql(
                "CREATE TABLE TestTable6 (`first` STRING PRIMARY KEY NOT ENFORCED, `second` String)");
        tableEnv.executeSql("INSERT INTO TestTable6 (SELECT `first`, `second` FROM  TestTable5)")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        validateSingleKey("TestTable6");
    }

    @Test
    public void testEmptyProjection() throws Exception {
        CollectionSink.output.clear();
        tableEnv.executeSql(
                "CREATE TABLE TestTableEP (`first` STRING PRIMARY KEY NOT ENFORCED, `second` STRING)");
        tableEnv.executeSql("INSERT INTO TestTableEP VALUES ('f','s')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);
        tableEnv.executeSql("INSERT INTO TestTableEP VALUES ('f2','s2')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        Table result = tableEnv.sqlQuery("SELECT COUNT(*) FROM TestTableEP");

        DataStream<Tuple2<Boolean, Row>> resultDataStream =
                tableEnv.toRetractStream(result, Types.ROW(Types.LONG));

        resultDataStream
                .map(t -> Tuple2.of(t.f0, t.f1.getField(0)))
                .returns(Types.TUPLE(Types.BOOLEAN, Types.LONG))
                .addSink(new CollectionSink<>())
                .setParallelism(1);

        resultDataStream.getExecutionEnvironment().execute();

        List<Tuple2<Boolean, Long>> expected =
                Lists.newArrayList(Tuple2.of(true, 1L), Tuple2.of(false, 1L), Tuple2.of(true, 2L));

        assertEquals(new HashSet<>(expected), new HashSet<>(CollectionSink.output));
        CollectionSink.output.clear();
    }

    @Test
    public void testTimestamp() throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE TestTableTsC (`first` STRING PRIMARY KEY NOT ENFORCED, `second` TIMESTAMP(3))");
        tableEnv.executeSql(
                        "INSERT INTO TestTableTsC values ('f', TIMESTAMP '2020-01-01 12:12:12.123456')")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        KuduTable kuduTable = getClient().openTable("TestTableTsC");
        assertEquals(Type.UNIXTIME_MICROS, kuduTable.getSchema().getColumn("second").getType());

        KuduScanner scanner = getClient().newScannerBuilder(kuduTable).build();
        List<RowResult> rows = new ArrayList<>();
        scanner.forEach(rows::add);

        assertEquals(1, rows.size());
        assertEquals("f", rows.get(0).getString(0));
        assertEquals(Timestamp.valueOf("2020-01-01 12:12:12.123"), rows.get(0).getTimestamp(1));
    }

    @Test
    public void testDatatypes() throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE TestTable8 (`first` STRING PRIMARY KEY NOT ENFORCED, `second` BOOLEAN, `third` BYTES,"
                        + "`fourth` TINYINT, `fifth` SMALLINT, `sixth` INT, `seventh` BIGINT, `eighth` FLOAT, `ninth` DOUBLE, "
                        + "`tenth` TIMESTAMP)");

        tableEnv.executeSql(
                        "INSERT INTO TestTable8 values ('f', false, cast('bbbb' as BYTES), cast(12 as TINYINT),"
                                + "cast(34 as SMALLINT), 56, cast(78 as BIGINT), cast(3.14 as FLOAT), cast(1.2345 as DOUBLE),"
                                + "TIMESTAMP '2020-04-15 12:34:56.123') ")
                .getJobClient()
                .get()
                .getJobExecutionResult()
                .get(1, TimeUnit.MINUTES);

        validateManyTypes("TestTable8");
    }

    @Test
    public void testMissingPropertiesCatalog() throws Exception {
        assertThrows(
                TableException.class,
                () ->
                        tableEnv.executeSql(
                                "CREATE TABLE TestTable9a (`first` STRING, `second` String) "
                                        + "WITH ('primary-key-columns' = 'second')"));
        assertThrows(
                TableException.class,
                () ->
                        tableEnv.executeSql(
                                "CREATE TABLE TestTable9b (`first` STRING, `second` String) "
                                        + "WITH ('hash-columns' = 'first')"));
        assertThrows(
                TableException.class,
                () ->
                        tableEnv.executeSql(
                                "CREATE TABLE TestTable9b (`first` STRING, `second` String) "
                                        + "WITH ('primary-key-columns' = 'second', 'hash-columns' = 'first')"));
    }

    private void validateManyTypes(String tableName) throws Exception {
        KuduTable kuduTable = getClient().openTable(tableName);
        Schema schema = kuduTable.getSchema();

        assertEquals(Type.STRING, schema.getColumn("first").getType());
        assertEquals(Type.BOOL, schema.getColumn("second").getType());
        assertEquals(Type.BINARY, schema.getColumn("third").getType());
        assertEquals(Type.INT8, schema.getColumn("fourth").getType());
        assertEquals(Type.INT16, schema.getColumn("fifth").getType());
        assertEquals(Type.INT32, schema.getColumn("sixth").getType());
        assertEquals(Type.INT64, schema.getColumn("seventh").getType());
        assertEquals(Type.FLOAT, schema.getColumn("eighth").getType());
        assertEquals(Type.DOUBLE, schema.getColumn("ninth").getType());
        assertEquals(Type.UNIXTIME_MICROS, schema.getColumn("tenth").getType());

        KuduScanner scanner = getClient().newScannerBuilder(kuduTable).build();
        List<RowResult> rows = new ArrayList<>();
        scanner.forEach(rows::add);

        assertEquals(1, rows.size());
        assertEquals("f", rows.get(0).getString(0));
        assertEquals(false, rows.get(0).getBoolean(1));
        assertEquals(ByteBuffer.wrap("bbbb".getBytes()), rows.get(0).getBinary(2));
        assertEquals(12, rows.get(0).getByte(3));
        assertEquals(34, rows.get(0).getShort(4));
        assertEquals(56, rows.get(0).getInt(5));
        assertEquals(78, rows.get(0).getLong(6));
        assertEquals(3.14, rows.get(0).getFloat(7), 0.01);
        assertEquals(1.2345, rows.get(0).getDouble(8), 0.0001);
        assertEquals(Timestamp.valueOf("2020-04-15 12:34:56.123"), rows.get(0).getTimestamp(9));
    }

    private void validateMultiKey(String tableName) throws Exception {
        KuduTable kuduTable = getClient().openTable(tableName);
        Schema schema = kuduTable.getSchema();

        assertEquals(2, schema.getPrimaryKeyColumnCount());
        assertEquals(3, schema.getColumnCount());

        assertTrue(schema.getColumn("first").isKey());
        assertTrue(schema.getColumn("second").isKey());

        assertFalse(schema.getColumn("third").isKey());

        KuduScanner scanner = getClient().newScannerBuilder(kuduTable).build();
        List<RowResult> rows = new ArrayList<>();
        scanner.forEach(rows::add);

        assertEquals(1, rows.size());
        assertEquals("f", rows.get(0).getString("first"));
        assertEquals(2, rows.get(0).getInt("second"));
        assertEquals("t", rows.get(0).getString("third"));
    }

    private static class CollectionSink<T> implements SinkFunction<T> {

        public static List<Object> output = Collections.synchronizedList(new ArrayList<>());

        public void invoke(T value, SinkFunction.Context context) {
            output.add(value);
        }
    }
}
