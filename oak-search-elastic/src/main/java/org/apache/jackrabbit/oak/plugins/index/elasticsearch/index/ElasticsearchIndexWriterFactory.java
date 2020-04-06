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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.index;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchConnection;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;

class ElasticsearchIndexWriterFactory implements FulltextIndexWriterFactory<ElasticsearchDocument> {
    private final ElasticsearchConnection elasticsearchConnection;

    ElasticsearchIndexWriterFactory(@NotNull ElasticsearchConnection elasticsearchConnection) {
        this.elasticsearchConnection = elasticsearchConnection;
    }

    @Override
    public ElasticsearchIndexWriter newInstance(IndexDefinition definition, NodeBuilder definitionBuilder, boolean reindex) {
        if (!(definition instanceof ElasticsearchIndexDefinition)) {
            throw new IllegalArgumentException("IndexDefinition must be of type ElasticsearchIndexDefinition " +
                    "instead of " + definition.getClass().getName());
        }
        return new ElasticsearchIndexWriter(elasticsearchConnection, (ElasticsearchIndexDefinition) definition);
    }
}
