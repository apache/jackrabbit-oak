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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;

import java.util.Set;

import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class LuceneInitializerHelper implements RepositoryInitializer {

    private final String name;

    private final Set<String> propertyTypes;

    public LuceneInitializerHelper(String name, Set<String> propertyTypes) {
        this.name = name;
        this.propertyTypes = propertyTypes;
    }

    @Override
    public NodeState initialize(NodeState state) {
        if (state.hasChildNode(INDEX_DEFINITIONS_NAME)
                && state.getChildNode(INDEX_DEFINITIONS_NAME)
                        .hasChildNode(name)) {
            return state;
        }
        NodeBuilder builder = state.builder();
        newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), name, propertyTypes);
        return builder.getNodeState();
    }

}
