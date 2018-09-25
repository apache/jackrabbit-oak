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
package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LucenePropertyIndex;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.spi.FilterReply;

/**
 * Test the Property2 index mechanism.
 */
public class LuceneIndexDeprecatedTest {

    private static final int MANY = 100;

    private NodeState root;
    private NodeBuilder rootBuilder;
    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(new LuceneIndexEditorProvider()));

    @Before
    public void setup() throws Exception {
        root = EmptyNodeState.EMPTY_NODE;
        rootBuilder = InitialContentHelper.INITIAL_CONTENT.builder();
        commit();
    }

    @Test
    public void deprecated() throws Exception {
        NodeBuilder index = newLucenePropertyIndexDefinition(
                rootBuilder.child(INDEX_DEFINITIONS_NAME),
                "foo", ImmutableSet.of("foo"), null);
        index.setProperty(IndexConstants.INDEX_DEPRECATED, false);
        commit();
        for (int i = 0; i < MANY; i++) {
            rootBuilder.child("test").child("n" + i).setProperty("foo", "x" + i % 20);
        }
        commit();

        FilterImpl f = createFilter(root, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("x10"));

        IndexTracker tracker = new IndexTracker();
        tracker.update(root);
        LucenePropertyIndex luceneIndex = new LucenePropertyIndex(tracker);
        IndexPlan plan = luceneIndex.getPlans(f, null, root).iterator().next();
        assertTrue(plan.getCostPerExecution() != Double.POSITIVE_INFINITY);
        ListAppender<ILoggingEvent> appender = createAndRegisterAppender();
        luceneIndex.query(plan, root);

        assertEquals("[]", appender.list.toString());
        appender.list.clear();

        // now test with the deprecated flag
        index = rootBuilder.child(INDEX_DEFINITIONS_NAME).child("foo");
        index.setProperty(IndexConstants.INDEX_DEPRECATED, true);
        index.setProperty("refresh", Boolean.TRUE);
        commit();

        appender.list.clear();

        // need to create a new one - otherwise the cached definition is used
        tracker = new IndexTracker();
        tracker.update(root);
        luceneIndex = new LucenePropertyIndex(tracker);
        plan = luceneIndex.getPlans(f, null, root).iterator().next();
        assertTrue(plan.getCostPerExecution() != Double.POSITIVE_INFINITY);
        luceneIndex.query(plan, root);

        assertEquals("[[WARN] This index is deprecated: /oak:index/foo; " +
                "it is used for query Filter(query=" +
                "SELECT * FROM [nt:base], path=*, property=[foo=[x10]]). " +
                "Please change the query or the index definitions.]", appender.list.toString());

        index = rootBuilder.child(INDEX_DEFINITIONS_NAME).child("foo");
        index.removeProperty(IndexConstants.INDEX_DEPRECATED);
        index.setProperty("refresh", Boolean.TRUE);
        commit();

        appender.list.clear();

        // need to create a new one - otherwise the cached definition is used
        tracker = new IndexTracker();
        tracker.update(root);
        luceneIndex = new LucenePropertyIndex(tracker);
        plan = luceneIndex.getPlans(f, null, root).iterator().next();
        assertTrue(plan.getCostPerExecution() != Double.POSITIVE_INFINITY);
        luceneIndex.query(plan, root);

        assertEquals("[]", appender.list.toString());

        deregisterAppender(appender);
    }

    private void commit() throws Exception {
        root = HOOK.processCommit(rootBuilder.getBaseState(), rootBuilder.getNodeState(), EMPTY);
        rootBuilder = root.builder();
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private ListAppender<ILoggingEvent> createAndRegisterAppender() {
        WarnFilter filter = new WarnFilter();
        filter.start();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.setContext(getContext());
        appender.setName("TestLogCollector");
        appender.addFilter(filter);
        appender.start();
        rootLogger().addAppender(appender);
        return appender;
    }

    private void deregisterAppender(Appender<ILoggingEvent> appender){
        rootLogger().detachAppender(appender);
    }

    private static class WarnFilter extends ch.qos.logback.core.filter.Filter<ILoggingEvent> {

        @Override
        public FilterReply decide(ILoggingEvent event) {
            if (event.getLevel().isGreaterOrEqual(Level.WARN)) {
                return FilterReply.ACCEPT;
            } else {
                return FilterReply.DENY;
            }
        }
    }

    private static LoggerContext getContext(){
        return (LoggerContext) LoggerFactory.getILoggerFactory();
    }

    private static ch.qos.logback.classic.Logger rootLogger() {
        return getContext().getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    }

    public void startCollecting() {
        Logger fooLogger = (Logger) LoggerFactory.getLogger(LuceneIndexDeprecatedTest.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();

        // add the appender to the logger
        fooLogger.addAppender(listAppender);

        fooLogger.warn("hello");

        List<ILoggingEvent> logsList = listAppender.list;
        System.out.println(logsList);

    }

}
