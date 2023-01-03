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
package org.apache.jackrabbit.oak.plugins.index;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_QUERY_FILTER_REGEX;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_INCLUDED_PATHS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class IndexImproperUsageCommonTest extends AbstractQueryTest {

    protected ListAppender<ILoggingEvent> listAppender;

    protected NodeStore nodeStore;
    protected TestRepository repositoryOptionsUtil;
    protected Node indexNode;
    protected IndexOptions indexOptions;

    private static final String PATH_RESTRICTION_WARN_MESSAGE = "Index definition of index used have path restrictions and query won't return nodes from " +
            "those restricted paths";
    private static final String QUERY_FILTER_WARN_MESSAGE = "improper use of index /oak:index/%s with queryFilterRegex %s to search for value '%s'";

    @Before
    public void loggingAppenderStart() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        listAppender = new ListAppender<>();
        listAppender.start();
        context.getLogger("org.apache.jackrabbit.oak.query.QueryImpl").addAppender(listAppender);
    }

    @After
    public void loggingAppenderStop() {
        listAppender.stop();
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Test
    public void pathIncludeWithPathRestrictionsWarn() throws Exception {

        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.setProperty(createProperty(PROP_INCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        //Do not provide type information
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("a").addChild("b").setProperty("propa", 10);
        test.addChild("c").setProperty("propa", 10);
        root.commit();

        assertEventually(() -> {
            assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/a')"), containsString(indexOptions.getIndexType() + ":test1"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/a')", singletonList("/test/a/b"));
            // List appender should not have any warn logs as we are searching under right descendant as per path restrictions
            assertFalse(isWarnMessagePresent(listAppender, PATH_RESTRICTION_WARN_MESSAGE));
            assertTrue(explain("select [jcr:path] from [nt:base] where [propa] = 10").contains(indexOptions.getIndexType() + ":test1"));
            // List appender now will have warn log as we are searching under root(/) but index definition have include path restriction.
            assertTrue(isWarnMessagePresent(listAppender, PATH_RESTRICTION_WARN_MESSAGE));
        });
    }

    @Test
    public void pathExcludeWithPathRestrictionsWarn() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        //Do not provide type information
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("a").addChild("b").setProperty("propa", 10);
        test.addChild("c").setProperty("propa", 10);
        test.addChild("c").addChild("d").setProperty("propa", 10);
        root.commit();

        assertEventually(() -> {
            assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c')"), containsString(indexOptions.getIndexType() + ":test1"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c')", singletonList("/test/c/d"));
            // List appender should not have any warn logs as we are searching under right descendant as per path restrictions
            assertFalse(isWarnMessagePresent(listAppender, PATH_RESTRICTION_WARN_MESSAGE));
            assertTrue(explain("select [jcr:path] from [nt:base] where [propa] = 10").contains(indexOptions.getIndexType() + ":test1"));
            // List appender now will have warn log as we are searching under root(/) but index definition have exclude path restriction.
            assertTrue(isWarnMessagePresent(listAppender, PATH_RESTRICTION_WARN_MESSAGE));
        });
    }

    @Test
    public void queryFilterRegexRestrictionsWarn() throws Exception {

        final String regex = "o.*";
        final String indexName = "test1";
        Tree idx = createIndex(indexName, of("propa", "propb"));
        idx.setProperty(PROP_QUERY_FILTER_REGEX, regex);
        idx.setProperty("tags", of("testtag"), Type.STRINGS);

        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "oak");
        test.addChild("b").setProperty("propb", "on");
        root.commit();

        assertEventually(() -> {
            assertTrue(explain("select [jcr:path] from [nt:base] where [propa] = \"oak\"").contains(indexOptions.getIndexType() + ":" + indexName));
            // List appender should not have any warn logs as we are searching using a term matching regex
            assertFalse(isWarnMessagePresent(listAppender ,String.format(QUERY_FILTER_WARN_MESSAGE, indexName, regex, "oak")));
            
            assertTrue(explain("select [jcr:path] from [nt:base] where [propa] = \"ack\"").contains(indexOptions.getIndexType() + ":" + indexName));
            // List appender now still have warn log as property restriction does not match regex restriction.
            assertTrue(isWarnMessagePresent(listAppender, String.format(QUERY_FILTER_WARN_MESSAGE, indexName, regex, "ack")));
          
            assertTrue(explain("select [jcr:path] from [nt:base] where [propa] = \"oak\" option (index tag testtag)").contains(indexOptions.getIndexType() + ":" + indexName));
            // List appender should not have any warn logs as the property restrictions which does not match regex restriction starts with ":" so is ignored
            assertFalse(isWarnMessagePresent(listAppender, String.format(QUERY_FILTER_WARN_MESSAGE, indexName, regex, "testtag")));
        
        });
    }

    private boolean isWarnMessagePresent(ListAppender<ILoggingEvent> listAppender, String warnMessage) {
        for (ILoggingEvent loggingEvent : listAppender.list) {
            if (loggingEvent.getMessage().contains(warnMessage)) {
                return true;
            }
        }
        return false;
    }

    private String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    private Tree createIndex(String name, Set<String> propNames) throws CommitFailedException {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }
    

    public Tree createIndex(Tree index, String name, Set<String> propNames) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));

        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    private static void assertEventually(Runnable r) {
        TestUtil.assertEventually(r, 3000 * 3);
    }

}
