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

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.apache.jackrabbit.oak.query.QueryOptimizerTest.rule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryOptimizerQueryTest {

    private static final String SQL2 = "select * from [nt:base] where [old] = $value order by [jcr:path]";
    private final MemoryNodeStore store = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
    private final AtomicLong clock = new AtomicLong();
    private final QueryOptimizer optimizer = new QueryOptimizer(clock::get);
    private final QueryEngineSettings settings = new QueryEngineSettings() {
        @Override
        public QueryOptimizer getQueryOptimizer() {
            return optimizer;
        }

        @Override
        public long getQueryLengthErrorLimit() {
            return 500;
        }

        @Override
        public long getQueryLengthWarnLimit() {
            return 250;
        }
    };
    private ContentRepository repository;
    private ContentSession session;
    private QueryEngine engine;
    private FeatureToggle toggle;
    private String previousProperty;

    @Before
    public void setUp() throws Exception {
        previousProperty = System.getProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER);
        System.clearProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER);
        DefaultWhiteboard whiteboard = new DefaultWhiteboard();
        whiteboard.register(QueryEngineSettings.class, settings, Map.of());
        repository = new Oak(store).with(new OpenSecurityProvider())
                .with(whiteboard).createContentRepository();
        toggle = WhiteboardUtils.getServices(whiteboard, FeatureToggle.class).stream()
                .filter(feature -> QueryEngineSettings.FT_QUERY_OPTIMIZER.equals(feature.getName()))
                .findFirst().orElseThrow();
        session = repository.login(null, null);
        NodeBuilder root = store.getRoot().builder();
        for (String name : List.of("a", "b")) {
            root.child("test").child(name).setProperty("new", "value");
        }
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        engine = session.getLatestRoot().getQueryEngine();
        writeRules("[" + rule("old", "new") + "]");
    }

    @After
    public void tearDown() throws Exception {
        if (previousProperty == null) {
            System.clearProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER);
        } else {
            System.setProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER, previousProperty);
        }
        if (session != null) {
            session.close();
        }
        if (repository != null) {
            ((Closeable) repository).close();
        }
    }

    @Test
    public void eitherSystemPropertyOrToggleEnablesRewriting() throws Exception {
        assertEquals(List.of(), paths(sql2()));
        System.setProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER, "true");
        assertEquals(List.of("/test/a", "/test/b"), paths(sql2()));
        System.setProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER, "false");
        assertEquals(List.of(), paths(sql2()));
        toggle.setEnabled(true);
        assertEquals(List.of("/test/a", "/test/b"), paths(sql2()));
        System.setProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER, "true");
        assertEquals(List.of("/test/a", "/test/b"), paths(sql2()));
        toggle.setEnabled(false);
        assertEquals(List.of("/test/a", "/test/b"), paths(sql2()));
        System.clearProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER);
        assertEquals(List.of(), paths(sql2()));
        settings.setQueryOptimizerFeature(null);
        assertEquals(List.of(), paths(sql2()));
        System.setProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER, "true");
        assertEquals(List.of("/test/a", "/test/b"), paths(sql2()));
    }

    @Test
    public void systemPropertyEnablesRepositoryWithoutWhiteboard() throws Exception {
        System.setProperty(QueryEngineSettings.OAK_QUERY_OPTIMIZER, "true");
        ContentRepository other = new Oak(store).with(new OpenSecurityProvider()).createContentRepository();
        try (ContentSession otherSession = other.login(null, null)) {
            Result result = otherSession.getLatestRoot().getQueryEngine().executeQuery(SQL2, "JCR-SQL2",
                    Map.of("value", PropertyValues.newString("value")), NO_MAPPINGS);
            assertEquals(List.of("/test/a", "/test/b"), paths(result));
        } finally {
            ((Closeable) other).close();
        }
    }

    @Test
    public void bindVariablesLimitsAndStatistics() throws Exception {
        toggle.setEnabled(true);
        assertEquals(List.of("value"), engine.getBindVariableNames(SQL2, "JCR-SQL2", NO_MAPPINGS));
        Result result = engine.executeQuery(SQL2, "JCR-SQL2", Optional.of(1L), Optional.of(1L),
                Map.of("value", PropertyValues.newString("value")), NO_MAPPINGS);
        assertEquals(List.of("/test/b"), paths(result));
        assertTrue(settings.getQueryStats().asJson().contains(SQL2));
        assertFalse(settings.getQueryStats().asJson().contains(SQL2.replace("old", "new")));
    }

    @Test
    public void xpathIsRewrittenBeforeConversionAndLanguageIsOptional() throws Exception {
        toggle.setEnabled(true);
        String xpath = "/jcr:root/test/*[@old='value']";
        assertEquals(List.of("/test/a", "/test/b"), paths(execute(xpath, "xpath")));
        assertEquals(List.of("/test/a", "/test/b"), paths(sql2()));
        writeRules("[{\"find\":\"old\",\"replace\":\"new\",\"language\":\"xpath\"}]");
        assertEquals(List.of("/test/a", "/test/b"), paths(execute(xpath, "xpath")));
        assertEquals(List.of(), paths(sql2()));
    }

    @Test
    public void sqlAndQomUseTheSameRewriter() throws Exception {
        toggle.setEnabled(true);
        for (String language : List.of("sql", "JCR-JQOM", "JCR-SQL2-noLiterals")) {
            Result result = engine.executeQuery(SQL2, language,
                    Map.of("value", PropertyValues.newString("value")), NO_MAPPINGS);
            assertEquals(List.of("/test/a", "/test/b"), paths(result));
        }
    }

    @Test
    public void committedConfigurationIsIndependentOfSessionRoot() throws Exception {
        toggle.setEnabled(true);
        assertEquals(List.of("/test/a", "/test/b"), paths(sql2()));
        writeRules("[]");
        assertEquals(List.of(), paths(sql2()));
    }

    @Test
    public void explainAndMeasureUseRewrittenQuery() throws Exception {
        toggle.setEnabled(true);
        for (String language : List.of("JCR-SQL2", "xpath")) {
            String statement = language.equals("xpath") ? "/jcr:root/test/*[@old='value']"
                    : "select * from [nt:base] where [old] = 'value'";
            Result explained = execute("explain " + statement, language);
            ResultRow row = explained.getRows().iterator().next();
            assertTrue(Arrays.asList(explained.getColumnNames()).contains("plan"));
            String effective = row.getValue("statement").getValue(Type.STRING);
            assertTrue(effective, effective.contains("new"));
            Result measured = execute("measure " + statement, language);
            ResultRow counts = measured.getRows().iterator().next();
            assertTrue(Arrays.asList(measured.getColumnNames()).contains("scanCount"));
            assertEquals(2, (long) counts.getValue("scanCount").getValue(Type.LONG));
        }
    }

    @Test
    public void internalQueriesRemainUnchanged() throws Exception {
        toggle.setEnabled(true);
        String internal = SQL2 + " " + QueryEngine.INTERNAL_SQL2_QUERY;
        assertEquals(List.of(), paths(engine.executeQuery(internal, "JCR-SQL2",
                Map.of("value", PropertyValues.newString("value")), NO_MAPPINGS)));
    }

    @Test
    public void queryValidatorChecksOriginalAndRewrittenStatements() throws Exception {
        toggle.setEnabled(true);
        settings.getQueryValidator().setPattern("original", ".*old.*", "", true);
        assertThrows(ParseException.class, this::sql2);
        settings.getQueryValidator().setPattern("original", "", "", true);
        settings.getQueryValidator().setPattern("rewritten", ".*new.*", "", true);
        assertThrows(ParseException.class, this::sql2);
    }

    @Test
    public void queryLengthAndLiteralRestrictionsStillApply() throws Exception {
        toggle.setEnabled(true);
        ParseException tooLong = assertThrows(ParseException.class,
                () -> engine.getBindVariableNames(SQL2 + " ".repeat(501), "JCR-SQL2", NO_MAPPINGS));
        assertTrue(tooLong.getMessage().contains("larger than max supported query length"));
        assertThrows(ParseException.class, () -> execute(
                "select * from [nt:base] where [old] = 'value'", "JCR-SQL2-noLiterals"));
        writeRules("[" + rule("\\$value", "'literal'") + "]");
        assertThrows(ParseException.class, () -> engine.getBindVariableNames(SQL2, "JCR-SQL2-noLiterals", NO_MAPPINGS));
        writeRules("[" + rule("old", "x".repeat(501)) + "]");
        assertThrows(ParseException.class, this::sql2);
        writeRules("[" + rule("old", "x".repeat(260)) + "]");
        assertEquals(List.of(), paths(sql2()));
    }

    @Test
    public void invalidRewrittenQueryIsNotSilentlyIgnored() throws Exception {
        toggle.setEnabled(true);
        writeRules("[" + rule("select", "invalid") + "]");
        assertThrows(ParseException.class, this::sql2);
    }

    private Result sql2() throws ParseException {
        return engine.executeQuery(SQL2, "JCR-SQL2",
                Map.of("value", PropertyValues.newString("value")), NO_MAPPINGS);
    }

    private Result execute(String statement, String language) throws ParseException {
        return engine.executeQuery(statement, language, NO_BINDINGS, NO_MAPPINGS);
    }

    private static List<String> paths(Result result) {
        List<String> paths = new ArrayList<>();
        for (ResultRow row : result.getRows()) {
            paths.add(row.getPath());
        }
        return paths;
    }

    private void writeRules(String json) throws Exception {
        NodeBuilder root = store.getRoot().builder();
        root.child("oak:index").child("diff.index.optimizer").child(QueryOptimizer.RULES_FILE)
                .child("jcr:content").setProperty(BinaryPropertyState.binaryProperty(
                        "jcr:data", json.getBytes(StandardCharsets.UTF_8)))
                .setProperty("jcr:lastModified", Instant.ofEpochMilli(clock.addAndGet(10_000)).toString(), Type.DATE);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
