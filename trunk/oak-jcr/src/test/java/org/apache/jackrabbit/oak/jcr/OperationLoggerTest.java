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

package org.apache.jackrabbit.oak.jcr;

import java.io.ByteArrayInputStream;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.slf4j.LoggerFactory;

/**
 * Testcase which asserts on some std log statements. These statement
 * are picked by tooling outside of Oak so act like a weak contract to
 * honour.
 */
public class OperationLoggerTest extends AbstractJCRTest {
    private final String[] OPS_LOGGERS = {
            "org.apache.jackrabbit.oak.jcr.operations"
    };
    private static final String OPS_QUERY = "org.apache.jackrabbit.oak.jcr.operations.query";
    private static final String OPS_BINARY = "org.apache.jackrabbit.oak.jcr.operations.binary";
    private ListAppender<ILoggingEvent> logs = new ListAppender<ILoggingEvent>();

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        stop();
    }

    public void testQueryLogger() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        //Log batch size is 100
        for (int i = 0; i < 200; i++) {
            node1.addNode("foo"+i, "oak:Unstructured").setProperty("foo", "bar");
        }
        superuser.save();

        QueryManager qm = superuser.getWorkspace().getQueryManager();
        start();
        String stmt = "select * from [nt:base] where foo = 'bar'";
        Query q = qm.createQuery(stmt, Query.JCR_SQL2);
        QueryResult r = q.execute();
        Iterators.size(r.getRows());
        stop();

        boolean queryStmtLog = false;
        boolean queryIterationLog = false;
        for (ILoggingEvent e : logs.list){
            if (OPS_QUERY.equals(e.getLoggerName())){
                if (e.getMessage().contains("Executed query")) {
                    assertEquals(stmt, e.getArgumentArray()[0]);
                    assertTrue(e.getArgumentArray()[1] instanceof Number);
                    queryStmtLog = true;
                }
                if (e.getMessage().contains("Iterated over")) {
                    queryIterationLog = true;
                }
            }
        }

        assertTrue("Did not find query log", queryStmtLog);
        assertTrue("Did not find query iteration log", queryIterationLog);
    }

    public void testBinaryLogger() throws Exception{
        Node node1 = testRootNode.addNode(nodeName1);

        start();
        byte[] data = "hello".getBytes();
        Binary b = superuser.getValueFactory().createBinary(new ByteArrayInputStream(data));
        node1.setProperty("foo", b);
        stop();

        boolean binaryLog = false;
        for (ILoggingEvent e : logs.list){
            if (e.getLoggerName().startsWith(OPS_BINARY)){
                if (e.getMessage().contains("Created binary property")) {
                    assertEquals(Long.valueOf(data.length), e.getArgumentArray()[0]);
                    binaryLog = true;
                }
            }
        }

        assertTrue("Did not find binary upload log", binaryLog);
    }

    private void start() {
        logs.start();
        logs.list.clear();
        for (String logger : OPS_LOGGERS) {
            getLogger(logger).addAppender(logs);
            getLogger(logger).setLevel(Level.DEBUG);
        }
    }

    private void stop() {
        for (String logger : OPS_LOGGERS) {
            getLogger(logger).detachAppender(logs);
            getLogger(logger).setLevel(null);
        }
        logs.stop();
    }

    private static Logger getLogger(String name) {
        return ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(name);
    }
}
