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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexDescendantSuggestionCommonTest;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.Node;
import javax.jcr.Repository;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EVALUATE_PATH_RESTRICTION;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;

public class LuceneIndexDescendantSuggestionCommonTest extends IndexDescendantSuggestionCommonTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Override
    protected Repository createJcrRepository() {
        indexOptions = new LuceneIndexOptions();
        repositoryOptionsUtil = new LuceneTestRepositoryBuilder(executorService, temporaryFolder).build();
        Oak oak = repositoryOptionsUtil.getOak();
        Jcr jcr = new Jcr(oak);
        return jcr.createRepository();
    }

    @After
    public void shutdownExecutor() {
        executorService.shutdown();
    }

    // this test does not make sense in ES since paths are always stored
    //OAK-3994
    @Test
    public void descendantSuggestionRequirePathRestrictionIndex() throws Exception {
        Node rootIndexDef = root.getNode("oak:index/sugg-idx");
        rootIndexDef.getProperty(EVALUATE_PATH_RESTRICTION).remove();
        rootIndexDef.setProperty(REINDEX_PROPERTY_NAME, true);
        session.save();

        //Without path restriction indexing, descendant clause shouldn't be respected
        validateSuggestions(
                createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content1"),
                newHashSet("test1", "test2", "test3", "test4", "test5", "test6"));
    }

}
