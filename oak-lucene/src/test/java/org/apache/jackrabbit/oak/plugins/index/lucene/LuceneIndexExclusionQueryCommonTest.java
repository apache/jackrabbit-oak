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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexExclusionQueryCommonTest;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;

import static com.google.common.collect.ImmutableList.of;
import static javax.jcr.PropertyType.TYPENAME_BINARY;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.useV2;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_TYPES;

/**
 * Tests the {@link LuceneIndexProvider} exclusion settings
 */
public class LuceneIndexExclusionQueryCommonTest extends IndexExclusionQueryCommonTest {

    private static final String NOT_IN = "notincluded";

    @Override
    protected void createTestIndexNode() throws Exception {
        indexOptions = new LuceneIndexOptions();
        Tree lucene = createTestIndexNode(root.getTree("/"), TYPE_LUCENE);
        lucene.setProperty(INCLUDE_PROPERTY_TYPES,
                of(TYPENAME_BINARY, TYPENAME_STRING), STRINGS);
        lucene.setProperty(EXCLUDE_PROPERTY_NAMES, of(NOT_IN), STRINGS);
        useV2(lucene);
        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        LowCostLuceneIndexProvider provider = new LowCostLuceneIndexProvider();
        return new Oak(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT))
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(new LuceneIndexEditorProvider())
                .createContentRepository();
    }

}
