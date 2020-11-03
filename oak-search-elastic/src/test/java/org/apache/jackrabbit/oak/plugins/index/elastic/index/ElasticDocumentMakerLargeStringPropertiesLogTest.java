/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextDocumentMaker;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidParameterException;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticDocumentMakerLargeStringPropertiesLogTest {

    ListAppender<ILoggingEvent> listAppender = null;
    private final String nodeImplLogger = ElasticDocumentMaker.class.getName();
    private final String warnMessage = "String length: {} for property: {} at Node: {} is greater than configured value {}";
    private String customStringPropertyThresholdLimit = "9";
    private String smallStringProperty = "1234567";
    private String largeStringPropertyAsPerCustomThreshold = "1234567890";

    @Rule
    public TemporarySystemProperty temporarySystemProperty = new TemporarySystemProperty();

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

    private void setThresholdLimit(String threshold) {
        System.setProperty(FulltextDocumentMaker.WARN_LOG_STRING_SIZE_THRESHOLD_KEY, threshold);
    }

    private ElasticDocumentMaker addPropertyAccordingToType(NodeBuilder test, Type type, String... str) throws IOException {
        NodeState root = INITIAL_CONTENT;
        ElasticIndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .analyzed()
                .valueExcludedPrefixes("/jobs");

        IndexDefinition defn = ElasticIndexDefinition.newBuilder(root, builder.build(), "/foo").build();
        ElasticDocumentMaker docMaker = new ElasticDocumentMaker(null, defn,
                defn.getApplicableIndexingRule("nt:base"), "/x");

        if (Type.STRINGS == type) {
            test.setProperty("foo", asList(str), Type.STRINGS);
        } else if (Type.STRING == type && str.length == 1) {
            test.setProperty("foo", str[0]);
        } else {
            throw new InvalidParameterException();
        }
        return docMaker;
    }

    @Test
    public void testDefaultThreshold() throws IOException {
        NodeBuilder test = EMPTY_NODE.builder();
        ElasticDocumentMaker docMaker = addPropertyAccordingToType(test, Type.STRING, largeStringPropertyAsPerCustomThreshold);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertFalse(isWarnMessagePresent(listAppender));
    }

    @Test
    public void testNoLoggingOnAddingSmallStringWithCustomThreshold() throws IOException {
        setThresholdLimit(customStringPropertyThresholdLimit);
        NodeBuilder test = EMPTY_NODE.builder();
        ElasticDocumentMaker docMaker = addPropertyAccordingToType(test, Type.STRING, smallStringProperty);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertFalse(isWarnMessagePresent(listAppender));
    }

    @Test
    public void testNoLoggingOnAddingSmallStringArrayWithCustomThreshold() throws IOException {
        setThresholdLimit(customStringPropertyThresholdLimit);
        NodeBuilder test = EMPTY_NODE.builder();
        ElasticDocumentMaker docMaker = addPropertyAccordingToType(test, Type.STRINGS, smallStringProperty, smallStringProperty);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertFalse(isWarnMessagePresent(listAppender));
    }

    @Test
    public void testNoLoggingOnAddingSmallStringArrayWithoutCustomThreshold() throws IOException {
        NodeBuilder test = EMPTY_NODE.builder();
        ElasticDocumentMaker docMaker = addPropertyAccordingToType(test, Type.STRINGS, smallStringProperty, smallStringProperty);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertFalse(isWarnMessagePresent(listAppender));
    }

    @Test
    public void testLoggingOnAddingLargeStringWithCustomThreshold() throws IOException {
        setThresholdLimit(customStringPropertyThresholdLimit);
        NodeBuilder test = EMPTY_NODE.builder();
        ElasticDocumentMaker docMaker = addPropertyAccordingToType(test, Type.STRING, largeStringPropertyAsPerCustomThreshold);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertTrue(isWarnMessagePresent(listAppender));
    }

    @Test
    public void testLoggingOnAddingLargeStringWithoutCustomThreshold() throws IOException {
        //  setThresholdLimit(null);
        NodeBuilder test = EMPTY_NODE.builder();
        ElasticDocumentMaker docMaker = addPropertyAccordingToType(test, Type.STRING, smallStringProperty);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertFalse(isWarnMessagePresent(listAppender));
    }

    @Test
    public void testLoggingOnAddingLargeStringArrayOneLargePropertyWithoutCustomThreshold() throws IOException {
        NodeBuilder test = EMPTY_NODE.builder();
        ElasticDocumentMaker docMaker = addPropertyAccordingToType(test, Type.STRINGS, largeStringPropertyAsPerCustomThreshold, smallStringProperty);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertFalse(isWarnMessagePresent(listAppender));
    }

    @Test
    public void testLoggingOnAddingLargeStringArrayOneLargePropertyWithCustomThreshold() throws IOException {
        setThresholdLimit(customStringPropertyThresholdLimit);
        NodeBuilder test = EMPTY_NODE.builder();
        ElasticDocumentMaker docMaker = addPropertyAccordingToType(test, Type.STRINGS, largeStringPropertyAsPerCustomThreshold, smallStringProperty);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertTrue(isWarnMessagePresent(listAppender));
        assertEquals(2, listAppender.list.size());
    }

    @Test
    public void testLoggingOnAddingLargeStringArrayTwoLargePropertiesWithCustomThreshold() throws IOException {
        setThresholdLimit(customStringPropertyThresholdLimit);
        NodeBuilder test = EMPTY_NODE.builder();
        ElasticDocumentMaker docMaker = addPropertyAccordingToType(test, Type.STRINGS, largeStringPropertyAsPerCustomThreshold, largeStringPropertyAsPerCustomThreshold);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));
        assertTrue(isWarnMessagePresent(listAppender));
        // number of logs equal twice the number of large properties once for fulltext indexing
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