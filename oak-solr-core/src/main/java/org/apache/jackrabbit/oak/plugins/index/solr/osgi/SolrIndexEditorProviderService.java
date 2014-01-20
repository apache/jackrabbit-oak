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

import static org.apache.felix.scr.annotations.ReferencePolicy.STATIC;
import static org.apache.felix.scr.annotations.ReferencePolicyOption.GREEDY;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Osgi Service that provides Solr based {@link org.apache.jackrabbit.oak.plugins.index.IndexEditor}s
 * 
 * @see org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider
 * @see org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider
 */
@Component(metatype = false, immediate = true)
@Service(value = IndexEditorProvider.class)
public class SolrIndexEditorProviderService implements IndexEditorProvider {
    
    @Reference(policyOption = GREEDY, policy = STATIC)
    private SolrServerProvider solrServerProvider;

    @Reference(policyOption = GREEDY, policy = STATIC)
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    private SolrIndexEditorProvider solrIndexEditorProvider;

    @Override
    @CheckForNull
    public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition, 
                    @Nonnull NodeState root, IndexUpdateCallback callback) throws CommitFailedException {
        Editor indexEditor = null;
        if (solrServerProvider != null && oakSolrConfigurationProvider != null && solrIndexEditorProvider == null) {
            solrIndexEditorProvider = new SolrIndexEditorProvider(solrServerProvider, oakSolrConfigurationProvider);
            indexEditor = solrIndexEditorProvider.getIndexEditor(type, definition, root, callback);
        }
        return indexEditor;
    }

}
