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

package org.apache.flink.connector.kudu.connector.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.kudu.connector.reader.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduSource implements Source, ResultTypeQueryable {
    private final String kuduMasterAddresses;
    private final String kuduTableName;
    private final Logger log = LoggerFactory.getLogger(getClass());

    public KuduSource(
            String kuduMasterAddresses,
            String kuduTableName
    ) {
       this.kuduMasterAddresses = kuduMasterAddresses;
       this.kuduTableName = kuduTableName;

       log.info("KuduSource initialized.");
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator createEnumerator(SplitEnumeratorContext enumContext) throws Exception {
        return new KuduSplitEnumerator(enumContext, kuduMasterAddresses, kuduTableName);

    }

    @Override
    public SplitEnumerator restoreEnumerator(SplitEnumeratorContext enumContext, Object checkpoint) throws Exception {
        return new KuduSplitEnumerator(enumContext, kuduMasterAddresses, kuduTableName);
    }

    @Override
    public SimpleVersionedSerializer getSplitSerializer() {
        return new KuduSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer getEnumeratorCheckpointSerializer() {
        return new KuduSourceEnumStateSerializer();
    }

    @Override
    public SourceReader createReader(SourceReaderContext readerContext) throws Exception {
        return new KuduSplitReader(readerContext, kuduMasterAddresses);
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(String.class);
    }
}
