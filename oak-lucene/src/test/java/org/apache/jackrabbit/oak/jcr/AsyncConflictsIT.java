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
package org.apache.jackrabbit.oak.jcr;

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.spi.FilterReply;

import com.google.common.collect.ImmutableSet;

public class AsyncConflictsIT extends DocumentClusterIT {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
    private static final String INDEX_DEF_NODE = "asyncconflict";
    private static final String INDEX_PROPERTY = "number";
    private static final Logger LOG = LoggerFactory.getLogger(AsyncConflictsIT.class);
    
    @BeforeClass
    public static void assumptions() {
        assumeTrue(FIXTURES.contains(Fixture.DOCUMENT_NS));
        assumeTrue(OakMongoNSRepositoryStub.isMongoDBAvailable());
    }
    
    @Test
    public void updates() throws Exception {
        final Map<String, Exception> exceptions = Collections.synchronizedMap(new HashMap<String, Exception>());
        final Random generator = new Random(3);
        final ListAppender<ILoggingEvent> logAppender = subscribeAppender();
        
        setUpCluster(this.getClass(), mks, repos, NOT_PROVIDED);
        defineIndex(repos.get(0));
                
        final int numberNodes = 10000;
        
        LOG.info("adding {} nodes", numberNodes);
        Session s = repos.get(0).login(ADMIN);
        Node test = s.getRootNode().addNode("test");
        test.setPrimaryType(NT_OAK_UNSTRUCTURED);
        
        try {
            for (int i = 0; i < numberNodes; i++) {
                test.addNode("node" + i);
                test.setProperty(INDEX_PROPERTY, generator.nextInt(numberNodes/3));
                if (i % 1024 == 0) {
                    s.save();
                }
            }
            
            s.save();
        } catch (Exception e) {
            exceptions.put(Thread.currentThread().getName(), e);
        } finally {
            s.logout();
        }
        
        LOG.info("Nodes added.");
        
        // issuing re-index
        LOG.info("issuing re-index and wait for finish");
        s = repos.get(0).login(ADMIN);
        try {
            Node index = s.getNode("/oak:index/" + INDEX_DEF_NODE);
            index.setProperty(REINDEX_PROPERTY_NAME, true);
            s.save();
        } catch (Exception e) {
            exceptions.put(Thread.currentThread().getName(), e);
        } finally {
            s.logout();
        }
        while (!isReindexFinished()) {
            Thread.sleep(5000);
        }
        
        raiseExceptions(exceptions, LOG);
        
        // if following fails it means the Async index failed at least once.
        assertTrue(
            String.format("We should have not any '%s' in the logs", AsyncLogFilter.MESSAGE),
            logAppender.list.isEmpty());
        
        unsubscribe(logAppender);
    }
    
    private boolean isReindexFinished() throws RepositoryException {
        Session s = repos.get(0).login(ADMIN);
        try {
            boolean reindex = s.getNode("/oak:index/" + INDEX_DEF_NODE)
                .getProperty(REINDEX_PROPERTY_NAME).getBoolean();
            return !reindex;
        } finally {
            s.logout();
        }
    }
    
    private void defineIndex(@Nonnull final Repository repo) throws RepositoryException {
        Session session = repo.login(ADMIN);
        try {
            Node n = session.getRootNode().getNode("oak:index");
            
            n = n.addNode(INDEX_DEF_NODE);
            n.setPrimaryType(IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
            n.setProperty("compatVersion", 2);
            n.setProperty(TYPE_PROPERTY_NAME, "lucene");
            n.setProperty(ASYNC_PROPERTY_NAME, "async");
            n = n.addNode("indexRules");
            n.setPrimaryType(NT_UNSTRUCTURED);
            n = n.addNode("nt:unstructured");
            n = n.addNode("properties");
            n.setPrimaryType(NT_UNSTRUCTURED);
            n = n.addNode("number");
            n.setPrimaryType(NT_UNSTRUCTURED);
            n.setProperty("propertyIndex", true);
            n.setProperty("name", INDEX_PROPERTY);
            
            session.save();
        } finally {
            session.logout();
        }
    }

    @Override
    protected Set<IndexEditorProvider> additionalIndexEditorProviders() {
        return ImmutableSet.of((IndexEditorProvider) new LuceneIndexEditorProvider());
    }

    @Override
    protected boolean isAsyncIndexing() {
        return true;
    }
    
    private ListAppender<ILoggingEvent> subscribeAppender() {
        Filter<ILoggingEvent> filter = new AsyncLogFilter();
        filter.start();
        ListAppender<ILoggingEvent> appender = new ListAppender<ILoggingEvent>();
        appender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        appender.setName("asynclogcollector");
        appender.addFilter(filter);
        appender.start();
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(
            ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME).addAppender(appender);
        return appender;
        
    }
    
    private void unsubscribe(@Nonnull final Appender<ILoggingEvent> appender) {
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(
            ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME).detachAppender(appender);
    }
    
    private static class AsyncLogFilter extends Filter<ILoggingEvent> {
        public static final String MESSAGE = "Unresolved conflicts in /:async";
        
        @Override
        public FilterReply decide(ILoggingEvent event) {
            final IThrowableProxy tp = event.getThrowableProxy();
            
            if (event.getLevel().isGreaterOrEqual(Level.WARN) &&
                tp != null && 
                tp.getMessage().contains(MESSAGE)) {
                return FilterReply.ACCEPT;
            } else {
                return FilterReply.DENY;
            }
        }
        
    }
}
