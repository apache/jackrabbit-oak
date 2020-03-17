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
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexUtil;
import org.apache.jackrabbit.oak.plugins.index.property.ValuePatternUtil;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.UniqueEntryStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_HEAD_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_PREVIOUS_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.uniquePropertyIndex;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexUtil.encode;

public class HybridPropertyIndexLookup {
    private static final Logger log = LoggerFactory.getLogger(HybridPropertyIndexLookup.class);
    private final String indexPath;
    private final NodeState indexState;
    private final String pathPrefix;
    private final boolean prependPathPrefix;

    public HybridPropertyIndexLookup(String indexPath, NodeState indexState) {
       this(indexPath, indexState, "", false);
    }

    public HybridPropertyIndexLookup(String indexPath, NodeState indexState,
                                     String pathPrefix, boolean prependPathPrefix) {
        this.indexPath = indexPath;
        this.indexState = indexState;
        this.pathPrefix = pathPrefix;
        this.prependPathPrefix = prependPathPrefix;
    }

    /**
     * Performs query based on provided property restriction
     *
     * @param filter filter from the query being performed
     * @param propertyName actual property name which may or may not be same as
     *                     property name in property restriction
     * @param restriction property restriction matching given property
     * @return iterable consisting of absolute paths as per index content
     */
    public Iterable<String> query(Filter filter, String propertyName, Filter.PropertyRestriction restriction) {
        //The propertyName may differ from name in restriction. For e.g. for relative properties
        //the restriction property name can be 'jcr:content/status' while the index has indexed
        //for 'status'

        Set<String> values = ValuePatternUtil.getAllValues(restriction);
        Set<String> encodedValues = PropertyIndexUtil.encode(values);
        return query(filter, propertyName, encodedValues);
    }

    public Iterable<String> query(Filter filter, String propertyName, PropertyValue value) {
        Set<String> values = Sets.newHashSet(value.getValue(Type.STRINGS));
        return query(filter, propertyName, encode(values));
    }

    private Iterable<String> query(Filter filter, String propertyName, Set<String> encodedValues) {
        String propIdxNodeName = HybridPropertyIndexUtil.getNodeName(propertyName);
        NodeState propIndexRootNode = indexState.getChildNode(PROPERTY_INDEX);
        NodeState propIndexNode = propIndexRootNode.getChildNode(propIdxNodeName);
        if (!propIndexNode.exists()) {
            return Collections.emptyList();
        }

        String indexName = indexPath + "(" + propertyName + ")";
        Iterable<String> result;
        if (uniquePropertyIndex(propIndexNode)) {
            result = queryUnique(filter, indexName, propIndexRootNode, propIdxNodeName, encodedValues);
        } else {
            result = querySimple(filter, indexName, propIndexNode, encodedValues);
        }

        Iterable<String> paths = transform(result, path -> isAbsolute(path) ? path : "/" + path);

        if (log.isTraceEnabled()) {
            paths = transform(paths, path -> {
                log.trace("[{}] {} = {} -> {}", indexPath, propertyName, encodedValues, path);
                return path;
            });
        }
        return paths;
    }

    private static Iterable<String> queryUnique(Filter filter, String indexName, NodeState propIndexRootNode,
                                         String propIdxNodeName, Set<String> values) {
        UniqueEntryStoreStrategy s = new UniqueEntryStoreStrategy(propIdxNodeName);
        return s.query(filter, indexName, propIndexRootNode, values);
    }

    private Iterable<String> querySimple(Filter filter, String indexName, NodeState propIndexNode,
                                                Set<String> values) {
        return Iterables.concat(
                queryBucket(filter, indexName, propIndexNode, PROP_HEAD_BUCKET, values),
                queryBucket(filter, indexName, propIndexNode, PROP_PREVIOUS_BUCKET, values)
        );
    }

    private Iterable<String> queryBucket(Filter filter, String indexName, NodeState propIndexNode,
                                         String bucketPropName, Set<String> values) {
        String bucketName = propIndexNode.getString(bucketPropName);
        if (bucketName == null) {
            return Collections.emptyList();
        }

        NodeState bucket = propIndexNode.getChildNode(bucketName);
        if (bucket.getChildNodeCount(1) == 0) {
            return Collections.emptyList();
        }

        ContentMirrorStoreStrategy s = new ContentMirrorStoreStrategy(bucketName, pathPrefix, prependPathPrefix);
        return s.query(filter, indexName, propIndexNode, bucketName, values);
    }
}
