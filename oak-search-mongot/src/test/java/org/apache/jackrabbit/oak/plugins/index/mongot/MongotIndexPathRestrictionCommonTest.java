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

import java.util.Collection;
import java.util.Collections;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.IndexPathRestrictionCommonTest;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongotIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.mongot.query.MongotIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.mongot.util.MongotIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.ClassRule;
import org.junit.runners.Parameterized;
import org.slf4j.event.Level;

public class MongotIndexPathRestrictionCommonTest extends IndexPathRestrictionCommonTest {

    private static final String MONGO_INDEX_LOGGER =
            "org.apache.jackrabbit.oak.plugins.index.mongot.query.MongotIndex";

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    private MongotIndexTracker tracker;

    public MongotIndexPathRestrictionCommonTest(boolean evaluatePathRestrictionsInIndex) {
        super(evaluatePathRestrictionsInIndex);
    }

    @Parameterized.Parameters(name = "evaluatePathRestrictionsInIndex = {0}")
    public static Collection<Object[]> data() {
        return Collections.singleton(doesIndexEvaluatePathRestrictions(true));
    }

    @Override
    protected void postCommitHooks() {
        tracker.update(root);
    }

    @Override
    protected void setupHook() {
        MongoConnection connection = mongo.getSearchConnection();
        tracker = new MongotIndexTracker(connection);
        hook = new EditorHook(new IndexUpdateProvider(
                new MongotIndexEditorProvider(connection, null)));
    }

    @Override
    protected void setupFullTextIndex() {
        tracker.update(root);
        index = (FulltextIndex) new MongotIndexProvider(tracker)
                .getQueryIndexes(null).get(0);
    }

    @Override
    protected IndexDefinitionBuilder getIndexDefinitionBuilder(NodeBuilder builder) {
        return new MongotIndexDefinitionBuilder(builder);
    }

    @Override
    protected String getExpectedLogEntryForPostPathFiltering(String path, boolean included) {
        return included
                ? String.format("Path %s satisfies hierarchy inclusion rules", path)
                : String.format("Path %s not included because of hierarchy inclusion rules", path);
    }

    @Override
    protected LogCustomizer getLogCustomizer() {
        return LogCustomizer.forLogger(MONGO_INDEX_LOGGER)
                .enable(Level.TRACE)
                .contains("hierarchy inclusion rules")
                .create();
    }
}
