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
package org.apache.jackrabbit.oak.plugins.tika;

import org.apache.commons.collections4.FluentIterable;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Traverser;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory.createReadOnlyTree;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;

import java.util.Objects;
import java.util.function.Function;

class NodeStoreBinaryResourceProvider implements BinaryResourceProvider {
    private static final Logger log = LoggerFactory.getLogger(NodeStoreBinaryResourceProvider.class);
    private final NodeStore nodeStore;
    private final BlobStore blobStore;

    public NodeStoreBinaryResourceProvider(NodeStore nodeStore, BlobStore blobStore) {
        this.nodeStore = nodeStore;
        this.blobStore = blobStore;
    }

    public FluentIterable<BinaryResource> getBinaries(String path) {
        // had to convert Guava's FluentIterable to Apache Commons Collections FluentIterable
        return Traverser
                .preOrderTraversal(createReadOnlyTree(getNode(nodeStore.getRoot(), path)), treeTraverser)
                .transform(new TreeToBinarySource()::apply)
                .filter(Objects::nonNull);
    }

    private class TreeToBinarySource implements Function<Tree, BinaryResource> {
        @Nullable
        @Override
        public BinaryResource apply(Tree tree) {
            PropertyState data = tree.getProperty(JcrConstants.JCR_DATA);
            if (data == null) {
                return null;
            }

            if (data.isArray()) {
                log.debug("Ignoring jcr:data property at {} as its a MVP", tree.getPath());
                return null;
            }

            Blob blob = data.getValue(Type.BINARY);
            String blobId = blob.getContentIdentity();
            //Check for ref being non null to ensure its not an inlined binary
            //For Segment ContentIdentity defaults to RecordId
            if (blob.getReference() == null || blobId == null) {
                log.debug("Ignoring jcr:data property at {} as its an inlined blob", tree.getPath());
                return null;
            }

            String mimeType = getString(tree, JcrConstants.JCR_MIMETYPE);
            String encoding = getString(tree, JcrConstants.JCR_ENCODING);

            return new BinaryResource(new BlobStoreByteSource(blobStore, blobId), mimeType,
                    encoding, tree.getPath(), blobId);
        }
    }

    final Function<Tree, Iterable<? extends Tree>> treeTraverser = Tree::getChildren;

    @Nullable
    private static String getString(Tree tree, String name) {
        PropertyState prop = tree.getProperty(name);
        return prop != null ? prop.getValue(Type.STRING) : null;
    }
}
