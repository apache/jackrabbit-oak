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

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.IndexPathRestrictionCommonTest;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.event.Level;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LuceneIndexPathRestrictionCommonTest extends IndexPathRestrictionCommonTest {

    private IndexTracker tracker;

    public LuceneIndexPathRestrictionCommonTest(boolean evaluatePathRestrictions) {
        super(evaluatePathRestrictions);
    }

    @Parameterized.Parameters(name = "evaluatePathRestrictionsInIndex = {0}")
    public static Collection<Object[]> data() {
        // For Lucene - evaluatePathRestrictions is used to enable/disable indexing ancestor paths.
        // So we test for both combinations here - evaluatePathRestrictions=true and  evaluatePathRestrictions=false
        return Arrays.asList(
                doesIndexEvaluatePathRestrictions(true),
                doesIndexEvaluatePathRestrictions(false));
    }

    @Override
    protected void postCommitHooks() {
        tracker.update(root);
    }

    @Override
    protected void setupHook() {
        tracker = new IndexTracker();
        hook = new EditorHook(new IndexUpdateProvider(new LuceneIndexEditorProvider()));
    }

    @Override
    protected void setupFullTextIndex() {
        tracker.update(root);
        index = new LucenePropertyIndex(tracker);
    }

    @Override
    protected IndexDefinitionBuilder getIndexDefinitionBuilder(NodeBuilder builder) {
        return new LuceneIndexDefinitionBuilder(builder);
    }

    @Override
    protected String getExpectedLogEntryForPostPathFiltering(String path, boolean shouldBeIncluded) {
        return String.format("Matched path %s; shouldIncludeForHierarchy: %s", path, shouldBeIncluded);
    }

    @Override
    protected LogCustomizer getLogCustomizer() {
        return LogCustomizer.forLogger(LucenePropertyIndex.class.getName()).enable(Level.TRACE).contains("Matched path").create();
    }
}
