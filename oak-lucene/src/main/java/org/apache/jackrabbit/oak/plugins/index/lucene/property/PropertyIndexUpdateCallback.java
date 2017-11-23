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

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.ValuePattern;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.UniqueEntryStoreStrategy;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Suppliers.ofInstance;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_CREATED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_HEAD_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_STORAGE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.STORAGE_TYPE_CONTENT_MIRROR;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.STORAGE_TYPE_UNIQUE;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexUtil.encode;

public class PropertyIndexUpdateCallback implements PropertyUpdateCallback {
    private static final Logger log = LoggerFactory.getLogger(PropertyIndexUpdateCallback.class);
    private static final String DEFAULT_HEAD_BUCKET = String.valueOf(1);

    private final NodeBuilder builder;
    private final String indexPath;
    private final UniquenessConstraintValidator uniquenessConstraintValidator;
    private final long updateTime;

    public PropertyIndexUpdateCallback(String indexPath, NodeBuilder builder, NodeState rootState) {
        this(indexPath, builder, rootState, Clock.SIMPLE);
    }

    public PropertyIndexUpdateCallback(String indexPath, NodeBuilder builder, NodeState rootState, Clock clock) {
        this.builder = builder;
        this.indexPath = indexPath;
        this.updateTime = clock.getTime();
        this.uniquenessConstraintValidator = new UniquenessConstraintValidator(indexPath, builder, rootState);
    }

    @Override
    public void propertyUpdated(String nodePath, String propertyRelativePath, PropertyDefinition pd,
                                @Nullable PropertyState before,  @Nullable PropertyState after) {
        if (!pd.sync) {
            return;
        }

        Set<String> beforeKeys = getValueKeys(before, pd.valuePattern);
        Set<String> afterKeys = getValueKeys(after, pd.valuePattern);

        //Remove duplicates
        Set<String> sharedKeys = newHashSet(beforeKeys);
        sharedKeys.retainAll(afterKeys);
        beforeKeys.removeAll(sharedKeys);
        afterKeys.removeAll(sharedKeys);

        if (!beforeKeys.isEmpty() || !afterKeys.isEmpty()){
            NodeBuilder indexNode = getIndexNode(propertyRelativePath, pd.unique);

            if (pd.unique) {
                UniqueEntryStoreStrategy s = new UniqueEntryStoreStrategy(INDEX_CONTENT_NODE_NAME,
                        (nb) -> nb.setProperty(PROP_CREATED, updateTime));
                s.update(ofInstance(indexNode),
                        nodePath,
                        null,
                        null,
                        beforeKeys,
                        afterKeys);
                uniquenessConstraintValidator.add(propertyRelativePath, afterKeys);
            } else {
                ContentMirrorStoreStrategy s = new ContentMirrorStoreStrategy();
                s.update(ofInstance(indexNode),
                        nodePath,
                        null,
                        null,
                        emptySet(), //Disable pruning with empty before keys
                        afterKeys);
            }

            if (log.isTraceEnabled()) {
                log.trace("[{}] Property index updated for [{}/@{}] with values {}", indexPath, nodePath,
                        propertyRelativePath, afterKeys);
            }
        }
    }

    @Override
    public void done() throws CommitFailedException {
        uniquenessConstraintValidator.validate();
    }

    public UniquenessConstraintValidator getUniquenessConstraintValidator() {
        return uniquenessConstraintValidator;
    }

    private NodeBuilder getIndexNode(String propertyRelativePath, boolean unique) {
        NodeBuilder propertyIndex = builder.child(PROPERTY_INDEX);

        if (propertyIndex.isNew()) {
            propertyIndex.setProperty(IndexConstants.REINDEX_RETAIN, true);
        }

        String nodeName = HybridPropertyIndexUtil.getNodeName(propertyRelativePath);
        if (unique) {
            return getUniqueIndexBuilder(propertyIndex, nodeName);
        } else {
            return getSimpleIndexBuilder(propertyIndex, nodeName);
        }
    }

    private NodeBuilder getSimpleIndexBuilder(NodeBuilder propertyIndex, String nodeName) {
        NodeBuilder idx = propertyIndex.child(nodeName);
        if (idx.isNew()) {
            idx.setProperty(PROP_HEAD_BUCKET, DEFAULT_HEAD_BUCKET);
            idx.setProperty(PROP_STORAGE_TYPE, STORAGE_TYPE_CONTENT_MIRROR);
        }

        String headBucketName = idx.getString(PROP_HEAD_BUCKET);
        checkNotNull(headBucketName, "[%s] property not found in [%s] for index [%s]",
                PROP_HEAD_BUCKET, idx, indexPath);

        return idx.child(headBucketName);
    }

    private static NodeBuilder getUniqueIndexBuilder(NodeBuilder propertyIndex, String nodeName) {
        NodeBuilder idx = propertyIndex.child(nodeName);
        if (idx.isNew()) {
            idx.setProperty(PROP_STORAGE_TYPE, STORAGE_TYPE_UNIQUE);
        }
        return idx;
    }

    private static Set<String> getValueKeys(PropertyState property, ValuePattern pattern) {
        Set<String> keys = new HashSet<>();
        if (property != null
                && property.getType().tag() != PropertyType.BINARY
                && property.count() != 0) {
            keys.addAll(encode(PropertyValues.create(property), pattern));
        }
        return keys;
    }
}
