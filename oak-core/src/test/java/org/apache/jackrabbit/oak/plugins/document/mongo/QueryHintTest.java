/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryHintTest extends AbstractMongoConnectionTest {
    final Logger TRACE_LOGGER = Logger.getLogger( "com.mongodb.TRACE" );
    final TestHandler testHandler = new TestHandler();

    private MongoDocumentStore mongoDS;

    private Clock clock;

    @Before
    public void prepareStores() throws Exception {
        clock = new Clock.Virtual();
        //TODO Temp mode to change the default setting so as to test it
        //If we retain this feature then need to have better config support for it
        System.setProperty("oak.mongo.maxDeltaForModTimeIdxSecs", "120");
        mongoDS = new MongoDocumentStore(mongoConnection.getDB(), new DocumentMK.Builder());
        mongoDS.setClock(clock);
        TRACE_LOGGER.addHandler(testHandler);
        TRACE_LOGGER.setLevel(Level.FINEST);
    }

    @Test
    public void testHints() throws Exception{
        assertFalse(mongoDS.getDisableIndexHint());

        long delta = mongoDS.getMaxDeltaForModTimeIdxSecs();
        clock.waitUntil(TimeUnit.SECONDS.toMillis(delta + 10));

        assertFalse(mongoDS.canUseModifiedTimeIdx(1));

        //For recently modified should be true
        assertTrue(mongoDS.canUseModifiedTimeIdx(10));

        mongoDS.query(Collection.NODES,
                Utils.getKeyLowerLimit("/"),
                Utils.getKeyUpperLimit("/"),
                NodeDocument.MODIFIED_IN_SECS,
                50,
                10);
        //TODO Use log message for better assert on
        //what hint is used
        //System.out.println(testHandler.records);
    }

    @After
    public void cleanup(){
        TRACE_LOGGER.removeHandler(testHandler);
        testHandler.close();
        TRACE_LOGGER.setLevel(null);
    }

    private static class TestHandler extends Handler {
        final List<String> records = Lists.newArrayList();

        @Override
        public void publish(LogRecord record) {
            String msg = record.getMessage();
            if(msg != null && msg.startsWith("find:")) {
                String json = msg.substring(msg.indexOf('{'));
                records.add(json);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() throws SecurityException {
            records.clear();
        }
    }
}
