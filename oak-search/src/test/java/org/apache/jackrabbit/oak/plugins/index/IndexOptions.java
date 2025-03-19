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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;

public abstract class IndexOptions {

    public abstract String getIndexType();

    protected Tree setIndex(Root root, String idxName, IndexDefinitionBuilder builder) {
        return builder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
    }

    protected Node setIndex(Session session, String idxName, IndexDefinitionBuilder builder) throws RepositoryException {
        return builder.build(session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).addNode(idxName, INDEX_DEFINITIONS_NODE_TYPE));
    }

    protected Node getIndexNode(Session session, String idxName) throws RepositoryException {
        return session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode(idxName);
    }

    protected IndexDefinitionBuilder createIndex(IndexDefinitionBuilder builder, boolean isAsync, String... propNames) {
        return createIndex(builder, "nt:base", isAsync, propNames);
    }

    protected IndexDefinitionBuilder createIndex(IndexDefinitionBuilder builder, String type, boolean isAsync, String... propNames) {
        if (!isAsync) {
            builder = builder.noAsync();
        }

        if (type != null) {
            IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule(type);
            for (String propName : propNames) {
                indexRule.property(propName).propertyIndex();
            }
        }
        return builder;
    }

    protected abstract IndexDefinitionBuilder createIndexDefinitionBuilder();

}
