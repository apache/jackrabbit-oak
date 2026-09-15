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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.mongodb.client.model.ReplaceOptions;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.IndexPlannerCommonTest;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongotIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.mongot.query.MongotIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.mongot.util.MongotIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.Document;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.junit.Assert.assertNotNull;

public class MongotIndexPlannerCommonTest extends IndexPlannerCommonTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    public MongotIndexPlannerCommonTest() {
        indexOptions = new MongotIndexOptions();
    }

    @After
    public void resetMongoDatabase() {
        mongo.useFreshDatabase();
    }

    @Override
    protected IndexNode createIndexNode(IndexDefinition definition) {
        return createIndexNode(definition, 1);
    }

    @Override
    protected IndexNode createIndexNode(IndexDefinition definition, long numOfDocs) {
        MongotIndexDefinition mongoDefinition = (MongotIndexDefinition) definition;
        for (long i = 0; i < numOfDocs; i++) {
            Document document = new Document("_id", "planner-" + i)
                    .append(MongoFieldNames.TYPED,
                            new Document(MongoFieldNames.encodeProperty("foo"), "bar"));
            mongo.getSearchConnection().getCollection(mongoDefinition).replaceOne(
                    new Document("_id", document.get("_id")), document,
                    new ReplaceOptions().upsert(true));
        }
        return indexNode(mongoDefinition);
    }

    @Override
    protected IndexDefinition getIndexDefinition(NodeState root, NodeState definition,
                                                 String indexPath) {
        return new MongotIndexDefinition(root, definition, indexPath);
    }

    @Override
    protected FulltextIndexPlanner getIndexPlanner(IndexNode indexNode, String indexPath,
                                                   Filter filter,
                                                   List<QueryIndex.OrderEntry> sortOrder) {
        return new MongotIndexPlanner(indexNode, indexPath, filter, sortOrder);
    }

    @Override
    protected IndexDefinitionBuilder getIndexDefinitionBuilder() {
        return new MongotIndexDefinitionBuilder();
    }

    @Override
    protected IndexDefinitionBuilder getIndexDefinitionBuilder(NodeBuilder builder) {
        return new MongotIndexDefinitionBuilder(builder);
    }

    @Override
    @Test
    public void worksWithIndexFormatV2Onwards() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder definition = getIndexDefinitionNodeBuilder(index, indexName,
                Set.of(TYPENAME_STRING));
        definition.child(INDEX_DATA_CHILD_NAME);

        IndexNode node = createIndexNode(getIndexDefinition(root, definition.getNodeState(),
                "/oak:index/" + indexName));
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));
        FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName,
                filter, Collections.emptyList());

        assertNotNull(planner.getPlan());
    }

    @Override
    protected ContentRepository createContentRepository(MemoryNodeStore store) {
        MongoConnection connection = mongo.getSearchConnection();
        MongotIndexTracker tracker = new MongotIndexTracker(connection);
        return new Oak(store)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new MongotIndexEditorProvider(connection,
                        new org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache(
                                10 * 1024 * 1024, 100)))
                .with(tracker)
                .createContentRepository();
    }

    @Override
    protected IndexNode getIndexNodeFromStore(String indexPath, NodeState root) {
        return new MongotIndexNode(root, indexPath, mongo.getSearchConnection());
    }

    private static IndexNode indexNode(MongotIndexDefinition definition) {
        MongoConnection connection = mongo.getSearchConnection();
        IndexStatistics statistics = new MongotIndexStatistics(connection, definition);
        return new IndexNode() {
            @Override
            public void release() {
            }

            @Override
            public IndexDefinition getDefinition() {
                return definition;
            }

            @Override
            public int getIndexNodeId() {
                return 0;
            }

            @Override
            public IndexStatistics getIndexStatistics() {
                return statistics;
            }
        };
    }
}
