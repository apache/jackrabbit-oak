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
package org.apache.jackrabbit.oak.upgrade;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateCopier;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.Nullable;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;

public final class IndexCopier {

    private IndexCopier() {
    }

    /**
     * Copy all index definition and data from sourceRoot to targetRoot. The
     * indexing data is filtered to include only content related to the passed
     * list of paths.
     *
     * @param sourceRoot the source
     * @param targetRoot the target
     * @param includes indexing data for these paths will be copied
     */
    public static void copy(NodeState sourceRoot, NodeBuilder targetRoot, Set<String> includes) {
        NodeState oakIndex = sourceRoot.getChildNode(INDEX_DEFINITIONS_NAME);
        NodeBuilder targetOakIndex = copySingleNode(oakIndex, targetRoot, INDEX_DEFINITIONS_NAME);

        for (ChildNodeEntry child : oakIndex.getChildNodeEntries()) {
            NodeState indexDef = child.getNodeState();
            String type = indexDef.getString(TYPE_PROPERTY_NAME);
            if (isEmpty(type)) {
                continue;
            }

            NodeBuilder targetIndexDef = copySingleNode(child, targetOakIndex);

            switch (type) {
                case "property":
                    if (indexDef.getBoolean(UNIQUE_PROPERTY_NAME)) {
                        copyUniqueIndex(indexDef, targetIndexDef, includes);
                    } else {
                        copyMirrorIndex(indexDef, targetIndexDef, includes);
                    }
                    break;

                case "counter":
                    copyMirrorIndex(indexDef, targetIndexDef, includes);

                case "lucene":
                    copyLuceneIndex(indexDef, targetIndexDef, includes);

                default:
                    break;
            }
        }
    }

    private static void copyUniqueIndex(NodeState indexDef, NodeBuilder targetIndexDef, Set<String> includes) {
        for (ChildNodeEntry childIndexNode : getIndexNodes(indexDef)) {
            NodeState indexNode = childIndexNode.getNodeState();
            NodeBuilder targetIndexNode = copySingleNode(indexNode, targetIndexDef, childIndexNode.getName());

            boolean anyAttrCopied = false;
            for (ChildNodeEntry attr : indexNode.getChildNodeEntries()) {
                Iterable<String> entries = attr.getNodeState().getStrings("entry");
                if (entries != null) {
                    for (String e : entries) {
                        if (startsWithAny(e, includes)) {
                            copySingleNode(attr, targetIndexNode);
                            anyAttrCopied = true;
                        }
                    }
                }
            }
            if (!anyAttrCopied) {
                targetIndexNode.remove();
            }
        }
    }

    private static void copyMirrorIndex(NodeState indexDef, NodeBuilder targetIndexDef, Set<String> includes) {
        for (ChildNodeEntry childIndexNode : getIndexNodes(indexDef)) {
            NodeState indexNode = childIndexNode.getNodeState();
            NodeBuilder targetIndexNode = copySingleNode(indexNode, targetIndexDef, childIndexNode.getName());

            boolean anyAttrCopied = false;
            for (ChildNodeEntry attr : indexNode.getChildNodeEntries()) {
                NodeBuilder targetAttr = copySingleNode(attr, targetIndexNode);
                boolean copied = NodeStateCopier.builder()
                        .include(includes)
                        .copy(attr.getNodeState(), targetAttr);
                if (!copied) {
                    targetAttr.remove();
                }
                anyAttrCopied = copied || anyAttrCopied;
            }
            if (!anyAttrCopied) {
                targetIndexNode.remove();
            }
        }
    }

    private static Iterable<? extends ChildNodeEntry> getIndexNodes(NodeState indexDef) {
        return Iterables.filter(indexDef.getChildNodeEntries(), new Predicate<ChildNodeEntry>() {
            @Override
            public boolean apply(@Nullable ChildNodeEntry input) {
                String name = input.getName();
                return name.equals(INDEX_CONTENT_NODE_NAME) || name.startsWith(":oak:mount-");
            }
        });
    }

    private static void copyLuceneIndex(NodeState indexDef, NodeBuilder targetIndexDef, Set<String> includes) {
        NodeStateCopier.builder()
                .include(singleton("/"))
                .copy(indexDef, targetIndexDef);
    }

    private static NodeBuilder copySingleNode(ChildNodeEntry source, NodeBuilder targetParent) {
        return copySingleNode(source.getNodeState(), targetParent, source.getName());
    }

    private static NodeBuilder copySingleNode(NodeState source, NodeBuilder targetParent, String name) {
        NodeBuilder target = targetParent.child(name);
        for (PropertyState p : source.getProperties()) {
            target.setProperty(p);
        }
        return target;
    }

    private static boolean startsWithAny(String subject, Iterable<String> patterns) {
        for (String p : patterns) {
            if (subject.startsWith(p)) {
                return true;
            }
        }
        return false;
    }
}
