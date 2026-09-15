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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.mongodb.client.MongoCollection;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongotIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.mongot.query.MongotIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.Document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class MongotTestRepositoryBuilder {

    private static final String INDEX_NAME = "mongoSearch";
    private static final String INDEX_PATH = "/oak:index/" + INDEX_NAME;

    private final MongotSearchConnectionRule mongo;
    private final NodeBuilder root;
    private final IndexDefinitionBuilder definition;

    public MongotTestRepositoryBuilder(MongotSearchConnectionRule mongo) {
        this.mongo = mongo;
        this.root = EmptyNodeState.EMPTY_NODE.builder();
        new InitialContent().initialize(root);
        root.child("oak:index").getChildNode("counter").remove();
        this.definition = new IndexDefinitionBuilder() {
            @Override
            protected String getIndexType() {
                return MongotIndexDefinition.TYPE_MONGOT;
            }
        };
        definition.evaluatePathRestrictions();
    }

    public NodeBuilder root() {
        return root;
    }

    public IndexDefinitionBuilder definition() {
        return definition;
    }

    public Fixture build() throws Exception {
        root.child("oak:index").setChildNode(INDEX_NAME, definition.build());
        MemoryNodeStore store = new MemoryNodeStore(root.getNodeState());
        MongoConnection connection = MongoConnection.create(
                mongo.getConnectionString(), mongo.getDatabaseName());
        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store,
                new MongotIndexEditorProvider(connection, null));
        async.run();
        assertFalse("Async Mongot indexing failed", async.isFailing());

        NodeState indexedRoot = store.getRoot();
        MongotIndexDefinition indexDefinition = new MongotIndexDefinition(indexedRoot,
                indexedRoot.getChildNode("oak:index").getChildNode(INDEX_NAME), INDEX_PATH);
        mongo.awaitSearchIndexReady(connection.getCollection(indexDefinition),
                indexDefinition.getSearchIndexName(), Duration.ofMinutes(2));

        MongotIndexTracker tracker = new MongotIndexTracker(connection);
        tracker.update(indexedRoot);
        ContentRepository repository = new Oak(store)
                .with(new OpenSecurityProvider())
                .with(new MongotIndexProvider(tracker))
                .createContentRepository();
        return new Fixture(connection, repository.login(null, null), store, async, tracker);
    }

    public static final class Fixture implements AutoCloseable {

        private final MongoConnection connection;
        private final ContentSession session;
        private final MemoryNodeStore store;
        private final AsyncIndexUpdate async;
        private final MongotIndexTracker tracker;

        private Fixture(MongoConnection connection,
                        ContentSession session,
                        MemoryNodeStore store,
                        AsyncIndexUpdate async,
                        MongotIndexTracker tracker) {
            this.connection = connection;
            this.session = session;
            this.store = store;
            this.async = async;
            this.tracker = tracker;
        }

        public void mutate(Consumer<NodeBuilder> mutation) throws CommitFailedException {
            NodeBuilder builder = store.getRoot().builder();
            mutation.accept(builder);
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        public void index() {
            async.run();
            assertFalse("Async Mongot indexing failed", async.isFailing());
            tracker.update(store.getRoot());
        }

        public MongoCollection<Document> collection() {
            return connection.getCollection(indexDefinition());
        }

        public MongotIndexDefinition indexDefinition() {
            NodeState current = store.getRoot();
            return new MongotIndexDefinition(current,
                    current.getChildNode("oak:index").getChildNode(INDEX_NAME), INDEX_PATH);
        }

        public Result query(String statement, String language) throws Exception {
            return query(statement, language, Long.MAX_VALUE, 0);
        }

        public Result query(String statement, String language, long limit, long offset) throws Exception {
            return session.getLatestRoot().getQueryEngine().executeQuery(statement, language,
                    limit, offset, QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        }

        public List<String> paths(String statement, String language) throws Exception {
            return paths(query(statement, language));
        }

        public List<String> paths(String statement, String language, long limit, long offset) throws Exception {
            return paths(query(statement, language, limit, offset));
        }

        public List<String> selectorPaths(String statement, String language, String selector) throws Exception {
            List<String> paths = new ArrayList<>();
            for (ResultRow row : query(statement, language).getRows()) {
                paths.add(row.getPath(selector));
            }
            return paths;
        }

        public void assertMongotPlan(String statement, String language) throws Exception {
            Result plan = query("explain " + statement, language);
            String value = plan.getRows().iterator().next()
                    .getValue("plan").getValue(Type.STRING);
            assertTrue(value, value.contains("mongot:"));
        }

        public Duration awaitPaths(String statement, String language, List<String> expected) throws Exception {
            long started = System.nanoTime();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            List<String> actual = List.of();
            do {
                actual = paths(statement, language);
                if (expected.equals(actual)) {
                    return Duration.ofNanos(System.nanoTime() - started);
                }
                TimeUnit.MILLISECONDS.sleep(25);
            } while (System.nanoTime() < deadline);
            assertEquals("Mongot results did not converge", expected, actual);
            throw new AssertionError("unreachable");
        }

        private static List<String> paths(Result result) {
            List<String> paths = new ArrayList<>();
            for (ResultRow row : result.getRows()) {
                paths.add(row.getPath());
            }
            return paths;
        }

        @Override
        public void close() throws Exception {
            try {
                async.close();
            } finally {
                try {
                    session.close();
                } finally {
                    connection.close();
                }
            }
        }
    }
}
