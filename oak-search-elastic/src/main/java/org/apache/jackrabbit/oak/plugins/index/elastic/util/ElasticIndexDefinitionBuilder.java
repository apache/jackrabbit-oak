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
package org.apache.jackrabbit.oak.plugins.index.elastic.util;

import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.TYPE_ELASTICSEARCH;

/**
 * Utility class to create Elasticsearch Index Definitions along with the relevant node structure.
 */
public class ElasticIndexDefinitionBuilder extends IndexDefinitionBuilder {
    @Override
    protected String getIndexType() {
        return TYPE_ELASTICSEARCH;
    }

    public ElasticIndexDefinitionBuilder() {
        super();
    }

    public ElasticIndexDefinitionBuilder(NodeBuilder builder) {
        super(builder);
    }
}
