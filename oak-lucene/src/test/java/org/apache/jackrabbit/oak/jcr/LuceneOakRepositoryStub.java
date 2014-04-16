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
package org.apache.jackrabbit.oak.jcr;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;

import java.util.Properties;
import java.util.Set;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.lucene.LowCostLuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.spi.commit.Observer;

public class LuceneOakRepositoryStub extends OakTarMKRepositoryStub {

    public LuceneOakRepositoryStub(Properties settings)
            throws RepositoryException {
        super(settings);
    }

    @Override
    protected void preCreateRepository(Jcr jcr) {
        LuceneIndexProvider provider = new LowCostLuceneIndexProvider();
        jcr.with(
                new LuceneInitializerHelper("luceneGlobal", (Set<String>) null))
                .with(AggregateIndexProvider.wrap(provider.with(getNodeAggregator())))
                .with((Observer) provider)
                .with(new LuceneIndexEditorProvider());
    }

    private static NodeAggregator getNodeAggregator() {
        return new SimpleNodeAggregator()
            .newRuleWithName(NT_FILE, newArrayList(JCR_CONTENT, JCR_CONTENT + "/*"));
    }
}
