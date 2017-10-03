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

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.UniqueEntryStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_HEAD_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_PREVIOUS_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexUtil.encode;

public class HybridPropertyIndexLookup {
    private final String indexPath;
    private final NodeState indexState;

    public HybridPropertyIndexLookup(String indexPath, NodeState indexState) {
        this.indexPath = indexPath;
        this.indexState = indexState;
    }

    public Iterable<String> query(Filter filter, PropertyDefinition pd,
                                  String propertyName, PropertyValue value) {

        String propIdxNodeName = HybridPropertyIndexUtil.getNodeName(propertyName);
        NodeState propIndexRootNode = indexState.getChildNode(PROPERTY_INDEX);
        NodeState propIndexNode = propIndexRootNode.getChildNode(propIdxNodeName);
        if (!propIndexNode.exists()) {
            return Collections.emptyList();
        }

        //TODO Check for non root indexes

        String indexName = indexPath + "(" + propertyName + ")";
        Set<String> values = encode(value, pd.valuePattern);
        if (pd.unique) {
            return queryUnique(filter, indexName, propIndexRootNode, propIdxNodeName, values);
        } else {
            return querySimple(filter, indexName, propIndexNode, values);
        }
    }

    private static Iterable<String> queryUnique(Filter filter, String indexName, NodeState propIndexRootNode,
                                         String propIdxNodeName, Set<String> values) {
        UniqueEntryStoreStrategy s = new UniqueEntryStoreStrategy(propIdxNodeName);
        return s.query(filter, indexName, propIndexRootNode, values);
    }

    private static Iterable<String> querySimple(Filter filter, String indexName, NodeState propIndexNode,
                                                Set<String> values) {
        return Iterables.concat(
                queryBucket(filter, indexName, propIndexNode, PROP_HEAD_BUCKET, values),
                queryBucket(filter, indexName, propIndexNode, PROP_PREVIOUS_BUCKET, values)
        );
    }

    private static Iterable<String> queryBucket(Filter filter, String indexName, NodeState propIndexNode,
                                         String bucketPropName, Set<String> values) {
        String bucketName = propIndexNode.getString(bucketPropName);
        if (bucketName == null) {
            return Collections.emptyList();
        }
        ContentMirrorStoreStrategy s = new ContentMirrorStoreStrategy(bucketName);
        return s.query(filter, indexName, propIndexNode, bucketName, values);
    }
}
