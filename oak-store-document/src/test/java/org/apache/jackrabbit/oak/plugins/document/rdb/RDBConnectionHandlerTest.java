/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.junit.Test;
import org.slf4j.event.Level;

public class RDBConnectionHandlerTest {

    @Test
    public void logging() throws Exception {
        LogCustomizer customLogs = LogCustomizer.forLogger(RDBConnectionHandler.class).enable(Level.TRACE)
                .contains("while obtaining new").create();
        DataSource ds = RDBDataSourceFactory.forJdbcUrl("jdbc:h2:mem:", "", "");
        Connection c1 = null, c2 = null, c3 = null;

        try (RDBConnectionHandler ch = new RDBConnectionHandler(ds)) {
            // warmup
            ch.getROConnection().close();
            // test
            customLogs.starting();
            long ts1 = System.currentTimeMillis();
            c1 = ch.getROConnection();
            assertTrue("There should be no log message yet, but got: " + customLogs.getLogs(), customLogs.getLogs().isEmpty());
            long ts2 = System.currentTimeMillis();
            assertTrue("unexpected elapsed time between two fetches of connections: " + (ts2 - ts1), ts2 - ts1 <= 20);
            c2 = ch.getROConnection();
            // age threshold not reached
            assertTrue("There should be no log message yet, but got: " + customLogs.getLogs(), customLogs.getLogs().isEmpty());
            while (System.currentTimeMillis() - ts2 < 101) {
                // busy wait for RDBConnectionHandler.LOGTHRESHOLD to pass
            }
            c3 = ch.getROConnection();
            assertEquals("There should be exacly one log message, but got: " + customLogs.getLogs(), 1, customLogs.getLogs().size());
        } finally {
            close(c1);
            close(c2);
            close(c3);
            customLogs.finished();
        }
    }

    private static void close(AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception ex) {
            }
        }
    }
}
