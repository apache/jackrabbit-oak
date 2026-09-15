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

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.IndexQuerySQL2OptimisationCommonTest;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongotIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.mongot.query.MongotIndexProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.ClassRule;

public class MongotIndexQuerySQL2OptimisationCommonTest
        extends IndexQuerySQL2OptimisationCommonTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    private final MongoConnection connection = mongo.getSearchConnection();
    private final MongotIndexTracker tracker = new MongotIndexTracker(connection);

    public MongotIndexQuerySQL2OptimisationCommonTest() {
        editorProvider = new MongotIndexEditorProvider(connection, null);
        indexProvider = new MongotIndexProvider(tracker);
    }

    @Override
    protected Oak getOakRepo() {
        indexOptions = new MongotIndexOptions();
        return new Oak(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT))
                .with(new OpenSecurityProvider())
                .with(indexProvider)
                .with(tracker)
                .with(editorProvider)
                .with(new QueryEngineSettings() {
                    @Override
                    public boolean isSql2Optimisation() {
                        return true;
                    }
                });
    }
}
