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
package org.apache.jackrabbit.oak.plugins.index.solr.osgi;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Osgi Service that provides Solr based {@link org.apache.jackrabbit.oak.plugins.index.IndexEditor}s
 *
 * @see org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider
 * @see org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider
 * <p>
 * @deprecated Solr support is deprecated and will be removed in a future version of Oak; see <a href=https://issues.apache.org/jira/browse/OAK-11314 target=_blank>Jira ticket OAK-11314</a> for more information.
 */
@Component(
        immediate = true,
        service = { IndexEditorProvider.class }
)
@Deprecated(forRemoval=true, since="1.74.0")
public class SolrIndexEditorProviderService implements IndexEditorProvider {

    @Reference
    private SolrServerProvider solrServerProvider;

    @Reference
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    @Override
    @Nullable
    public Editor getIndexEditor(@NotNull String type, @NotNull NodeBuilder definition,
                                 @NotNull NodeState root, @NotNull IndexUpdateCallback callback) throws CommitFailedException {
        if (solrServerProvider != null && oakSolrConfigurationProvider != null) {
            return new SolrIndexEditorProvider(solrServerProvider,
                    oakSolrConfigurationProvider).getIndexEditor(type, definition, root, callback);
        } else {
            return null;
        }
    }
}
