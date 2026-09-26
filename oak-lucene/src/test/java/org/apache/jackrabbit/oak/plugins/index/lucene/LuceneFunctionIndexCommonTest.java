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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.FunctionIndexCommonTest;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LuceneFunctionIndexCommonTest extends FunctionIndexCommonTest {

    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    private LuceneTestRepositoryBuilder luceneTestRepositoryBuilder;

    protected Tree createIndex(String name, Set<String> propNames) {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    protected Tree createIndex(Tree index, String name, Set<String> propNames) {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        def.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }


    @Override
    protected ContentRepository createRepository() {
        luceneTestRepositoryBuilder = new LuceneTestRepositoryBuilder(executorService, temporaryFolder);
        luceneTestRepositoryBuilder.setNodeStore(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT));
        repositoryOptionsUtil = luceneTestRepositoryBuilder.build();
        indexOptions = new LuceneIndexOptions();
        return repositoryOptionsUtil.getOak()
                .createContentRepository();
    }

    @Override
    protected String getLoggerName() {
        return LuceneIndexEditor.class.getName();
    }

    @After
    public void shutdownExecutor() {
        executorService.shutdown();
    }

    /**
     * An index whose only indexed property is a function that evaluates to
     * null for most nodes must stay "sparse": it should contain a Lucene
     * document only for the nodes where the function is not null, not one
     * document per node.
     */
    @Test
    public void sparseIndexForIfExistsFunction() throws CommitFailedException {
        Tree index = createIndex("aliasPath", Set.of());
        Tree func = index.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("aliasPath");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "if(exists([alias]), path(), null)");

        Tree test = root.getTree("/").addChild("test");
        int withAlias = 0;
        for (int idx = 0; idx < 10; idx++) {
            Tree n = test.addChild("n" + idx);
            n.setProperty(JcrConstants.JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
            if (idx % 3 == 0) {
                n.setProperty("alias", "a" + idx);
                withAlias++;
            }
        }
        root.commit();

        int expectedDocs = withAlias;
        assertEventually(() -> {
            LuceneIndexProvider provider = (LuceneIndexProvider) luceneTestRepositoryBuilder.getIndexProvider();
            LuceneIndexNode indexNode = provider.getTracker().acquireIndexNode("/oak:index/aliasPath");
            assertNotNull(indexNode);
            try {
                assertEquals(expectedDocs, indexNode.getSearcher().getIndexReader().numDocs());
            } finally {
                indexNode.release();
            }
        });
    }

}
