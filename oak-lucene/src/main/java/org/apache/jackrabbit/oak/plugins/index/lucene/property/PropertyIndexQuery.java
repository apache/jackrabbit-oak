/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_STORAGE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.STORAGE_TYPE_UNIQUE;

/**
 * Performs simple property=value query against a unique property index storage
 */
class PropertyIndexQuery implements PropertyQuery {
    private final NodeBuilder builder;

    public PropertyIndexQuery(NodeBuilder builder) {
        this.builder = builder;
    }

    @Override
    public Iterable<String> getIndexedPaths(String propertyRelativePath, String value) {
        NodeBuilder idxb = getIndexNode(propertyRelativePath);
        checkState(STORAGE_TYPE_UNIQUE.equals(idxb.getString(PROP_STORAGE_TYPE)));

        NodeBuilder entry = idxb.child(value);
        return entry.getProperty("entry").getValue(Type.STRINGS);
    }

    private NodeBuilder getIndexNode(String propertyRelativePath) {
        NodeBuilder propertyIndex = builder.child(PROPERTY_INDEX);
        String nodeName = HybridPropertyIndexUtil.getNodeName(propertyRelativePath);
        return propertyIndex.child(nodeName);
    }
}
