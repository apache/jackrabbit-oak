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

import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.junit.Test;
import org.slf4j.event.Level;

public class RDBConnectionHandlerTest {

    @Test
    public void logging() throws Exception {
        LogCustomizer customLogs = LogCustomizer.forLogger(RDBConnectionHandler.class.getName()).enable(Level.TRACE)
                .contains("while obtaining new").create();
        DataSource ds = RDBDataSourceFactory.forJdbcUrl("jdbc:h2:mem:", "", "");
        Connection c1 = null, c2 = null, c3 = null;

        try (RDBConnectionHandler ch = new RDBConnectionHandler(ds)) {
            customLogs.starting();
            c1 = ch.getROConnection();
            long ts = System.currentTimeMillis();
            assertEquals(0, customLogs.getLogs().size());
            c2 = ch.getROConnection();
            // age threshold not reached
            assertEquals(0, customLogs.getLogs().size());
            while (System.currentTimeMillis() - ts < 21) {
                // busy wait for LOGTHRESHOLD to pass
            }
            c3 = ch.getROConnection();
            assertEquals(1, customLogs.getLogs().size());
            // System.out.println(customLogs.getLogs());
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
