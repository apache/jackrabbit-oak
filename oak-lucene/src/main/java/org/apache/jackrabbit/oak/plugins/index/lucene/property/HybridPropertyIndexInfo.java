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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import com.google.common.collect.TreeTraverser;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_HEAD_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_PREVIOUS_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.simplePropertyIndex;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.uniquePropertyIndex;

public class HybridPropertyIndexInfo {
    private final JsopBuilder json = new JsopBuilder();
    private final NodeState idx;

    public HybridPropertyIndexInfo(NodeState idx) {
        this.idx = idx;
    }

    public String getInfoAsJson(){
        json.resetWriter();
        json.object();
        NodeState propertyIndexNode = idx.getChildNode(PROPERTY_INDEX);
        for (ChildNodeEntry cne : propertyIndexNode.getChildNodeEntries()) {
            NodeState propIdxState = cne.getNodeState();
            String propName = cne.getName();
            json.key(propName).object();
            if (simplePropertyIndex(propIdxState)) {
                collectBucketData(propIdxState);
            } else if (uniquePropertyIndex(propIdxState)) {
                json.key("entryCount").value(propIdxState.getChildNodeCount(Integer.MAX_VALUE));
                json.key("unique").value(true);
            }
            json.endObject();
        }
        json.endObject();
        return JsopBuilder.prettyPrint(json.toString());
    }

    private void collectBucketData(NodeState propIdxState) {
        String head = propIdxState.getString(PROP_HEAD_BUCKET);
        String previous = propIdxState.getString(PROP_PREVIOUS_BUCKET);

        for (ChildNodeEntry cne : propIdxState.getChildNodeEntries()) {
            String bucketName = cne.getName();
            NodeState bucket = cne.getNodeState();
            json.key(bucketName).object();

            json.key("type");
            if (Objects.equals(head, bucketName)) {
                json.value("head");
            } else if (Objects.equals(previous, bucketName)) {
                json.value("previous");
            } else {
                json.value("garbage");
            }

            json.key("keyCount").value(bucket.getChildNodeCount(Integer.MAX_VALUE));
            collectCounts(bucket);

            json.endObject();
        }
    }

    private void collectCounts(NodeState bucket) {
        TreeTraverser<NodeState> t = new TreeTraverser<NodeState>() {
            @Override
            public Iterable<NodeState> children(NodeState root) {
                return Iterables.transform(root.getChildNodeEntries(), ChildNodeEntry::getNodeState);
            }
        };
        AtomicInteger matches = new AtomicInteger();
        int totalCount = t.preOrderTraversal(bucket)
                .transform((st) -> {
                    if (st.getBoolean("match")) {
                        matches.incrementAndGet();
                    }
                    return st;
                }).size();
        json.key("entryCount").value(matches.get());
        json.key("totalCount").value(totalCount);
    }
}
