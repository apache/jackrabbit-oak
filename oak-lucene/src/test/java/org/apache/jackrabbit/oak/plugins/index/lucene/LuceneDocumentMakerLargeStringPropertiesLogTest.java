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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneDocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LuceneDocumentMakerLargeStringPropertiesLogTest {

    ListAppender<ILoggingEvent> listAppender = null;
    private final String nodeImplLogger = "org.apache.jackrabbit.oak.plugins.index.lucene.LuceneDocumentMaker";
    private final String warnMessage = "String length: {} for property: {} at Node: {} is greater than configured value {}";

    @Before
    public void loggingAppenderStart() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        listAppender = new ListAppender<>();
        listAppender.start();
        context.getLogger(nodeImplLogger).addAppender(listAppender);
    }

    @After
    public void loggingAppenderStop() {
        listAppender.stop();
    }

    @Test
    public void testNoLoggingOnAddingSmallString() throws IOException {
        NodeState root = INITIAL_CONTENT;
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .analyzed()
                .valueExcludedPrefixes("/jobs");

        LuceneIndexDefinition defn = LuceneIndexDefinition.newBuilder(root, builder.build(), "/foo").build();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(null, null, null, defn,
                defn.getApplicableIndexingRule("nt:base"), "/x", 9);

        NodeBuilder test = EMPTY_NODE.builder();

        test.setProperty("foo", "1234567");
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertFalse(isWarnMessagePresent(listAppender));
    }

    @Test
    public void testNoLoggingOnAddingSmallStringArray() throws IOException {
        NodeState root = INITIAL_CONTENT;
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .analyzed()
                .valueExcludedPrefixes("/jobs");

        LuceneIndexDefinition defn = LuceneIndexDefinition.newBuilder(root, builder.build(), "/foo").build();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(null, null, null, defn,
                defn.getApplicableIndexingRule("nt:base"), "/x", 9);

        NodeBuilder test = EMPTY_NODE.builder();

        test.setProperty("foo", asList("/a", "/jobs/a"), Type.STRINGS);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertFalse(isWarnMessagePresent(listAppender));

    }

    @Test
    public void testLoggingOnAddingLargeString() throws IOException {
        NodeState root = INITIAL_CONTENT;
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .analyzed()
                .valueExcludedPrefixes("/jobs");

        LuceneIndexDefinition defn = LuceneIndexDefinition.newBuilder(root, builder.build(), "/foo").build();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(null, null, null, defn,
                defn.getApplicableIndexingRule("nt:base"), "/x", 9);

        NodeBuilder test = EMPTY_NODE.builder();

        test.setProperty("foo", "1234567890");
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertTrue(isWarnMessagePresent(listAppender));

    }

    @Test
    public void testLoggingOnAddingLargeStringArrayOneLargeProperty() throws IOException {
        NodeState root = INITIAL_CONTENT;
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .analyzed()
                .valueExcludedPrefixes("/jobs");

        LuceneIndexDefinition defn = LuceneIndexDefinition.newBuilder(root, builder.build(), "/foo").build();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(null, null, null, defn,
                defn.getApplicableIndexingRule("nt:base"), "/x", 9);

        NodeBuilder test = EMPTY_NODE.builder();

        test.setProperty("foo", asList("/jobs/a", "1234567890"), Type.STRINGS);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertTrue(isWarnMessagePresent(listAppender));
        assertEquals(2, listAppender.list.size());

    }

    @Test
    public void testLoggingOnAddingLargeStringArrayTwoLargeProperties() throws IOException {
        NodeState root = INITIAL_CONTENT;
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .analyzed()
                .valueExcludedPrefixes("/jobs");

        LuceneIndexDefinition defn = LuceneIndexDefinition.newBuilder(root, builder.build(), "/foo").build();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(null, null, null, defn,
                defn.getApplicableIndexingRule("nt:base"), "/x", 9);

        NodeBuilder test = EMPTY_NODE.builder();

        test.setProperty("foo", asList("0123456789", "1234567890"), Type.STRINGS);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertTrue(isWarnMessagePresent(listAppender));
        // number of logs equal twice the number of large properties once for fultext indexing
        // and once for property indexing.
        assertEquals(4, listAppender.list.size());

    }

    private boolean isWarnMessagePresent(ListAppender<ILoggingEvent> listAppender) {
        for (ILoggingEvent loggingEvent : listAppender.list) {
            if (loggingEvent.getMessage().contains(warnMessage)) {
                return true;
            }
        }
        return false;
    }

}