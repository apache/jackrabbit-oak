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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Map;

public class DefaultElasticsearchIndexCoordinateFactory implements ElasticsearchIndexCoordinateFactory {
    private final Map<String, String> config;
    private final ElasticsearchConnectionFactory factory;

    DefaultElasticsearchIndexCoordinateFactory(ElasticsearchConnectionFactory factory, Map<String, String> config) {
        this.factory = factory;
        this.config = config;
    }

    @Override
    public ElasticsearchIndexCoordinate getElasticsearchIndexCoordinate(IndexDefinition indexDefinition) {
        ElasticsearchCoordinate esCoord = getElasticsearchCoordinate(indexDefinition.getDefinitionNodeState());
        return new ElasticsearchIndexCoordinateImpl(esCoord, indexDefinition);
    }

    private ElasticsearchCoordinate getElasticsearchCoordinate(NodeState indexDefinition) {
        return ElasticsearchCoordinateImpl.construct(factory, indexDefinition, config);
    }
}
