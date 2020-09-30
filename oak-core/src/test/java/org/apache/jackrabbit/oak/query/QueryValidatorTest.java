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
package org.apache.jackrabbit.oak.query;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;
import org.slf4j.event.Level;

/**
 * Tests the query validator.
 */
public class QueryValidatorTest {
    
    @Test
    public void empty() throws ParseException {
        QueryValidator v = new QueryValidator();
        // expected to be very fast
        v.checkStatement("x");
        v.setPattern("x", "x.*", "all", false);
        v.setPattern("x", "", "", false);
        v.checkStatement("x");
    }

    @Test
    public void warning() throws ParseException {
        QueryValidator v = new QueryValidator();
        v.setPattern("x", "x.*", "all", false);
        assertEquals("[\n" +
                "{\n" +
                "\"key\":\"x\"\n" +
                ",\"pattern\":\"x.*\"\n" +
                ",\"comment\":\"all\"\n" + 
                ",\"failQuery\":false\n" +
                ",\"executedLast\":\"\"\n" +
                ",\"executedCount\":0\n" +
                "}]", v.getJson());
        LogCustomizer customLogs = LogCustomizer.forLogger(QueryValidator.class.getName()).enable(Level.WARN).create();
        try {
            customLogs.starting();
            v.checkStatement("x1");
            v.checkStatement("x2");
            v.checkStatement("y");
            List<String> logs = customLogs.getLogs();
            assertEquals("[Query is questionable, but executed: statement=x1 pattern=x.*]", logs.toString());
        } finally {
            customLogs.finished();
        }        
    }

    @Test
    public void error() throws ParseException {
        QueryValidator v = new QueryValidator();
        v.setPattern("x", "x.*", "all", true);
        try {
            v.checkStatement("x1");
            fail();
        } catch (ParseException e) {
            // expected
        }
        v.checkStatement("y");
        assertTrue(v.getJson().startsWith("[\n" +
                "{\n" +
                "\"key\":\"x\"\n" +
                ",\"pattern\":\"x.*\"\n" +
                ",\"comment\":\"all\"\n" + 
                ",\"failQuery\":true\n"));
        assertTrue(v.getJson().indexOf("\"executedCount\":1") >= 0);
        v.checkStatement("y");
        try {
            v.checkStatement("x2");
            fail();
        } catch (ParseException e) {
            // expected
        }
        assertTrue(v.getJson().indexOf("\"executedCount\":2") >= 0);
    }
    
    @Test
    public void initFromNodeStore() throws CommitFailedException {
        QueryValidator v = new QueryValidator();
        MemoryNodeStore ns = new MemoryNodeStore();
        v.init(ns);
        assertEquals("[]", v.getJson());
        NodeBuilder rootBuilder = ns.getRoot().builder();
        NodeBuilder b;
        b = rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).
                child(QueryValidator.QUERY_VALIDATOR).child("ignored");
        b = rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).
            child(QueryValidator.QUERY_VALIDATOR).child("test");
        b.setProperty("pattern", "testPattern");
        b.setProperty("comment", "testComment");
        b.setProperty("failQuery", "true");
        ns.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        v.init(ns);
        assertEquals("[\n" +
                "{\n" +
                "\"key\":\"test\"\n" +
                ",\"pattern\":\"testPattern\"\n" +
                ",\"comment\":\"testComment\"\n" + 
                ",\"failQuery\":true\n" +
                ",\"executedLast\":\"\"\n" +
                ",\"executedCount\":0\n" +
                "}]",
                v.getJson());
        b = rootBuilder.child(IndexConstants.INDEX_DEFINITIONS_NAME).
                child(QueryValidator.QUERY_VALIDATOR).child("test");
            b.setProperty("pattern",  asList("select", "order by @x"), Type.STRINGS);
            b.setProperty("comment", "testComment");
            b.setProperty("failQuery", "true");
            ns.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            v.init(ns);
            assertEquals("[\n" +
                    "{\n" +
                    "\"key\":\"test\"\n" +
                    ",\"pattern\":\"\\\\Qselect\\\\E.*\\\\Qorder by @x\\\\E\"\n" + 
                    ",\"comment\":\"testComment\"\n" + 
                    ",\"failQuery\":true\n" +
                    ",\"executedLast\":\"\"\n" +
                    ",\"executedCount\":0\n" +
                    "}]",
                    v.getJson());        
    }
    
}
