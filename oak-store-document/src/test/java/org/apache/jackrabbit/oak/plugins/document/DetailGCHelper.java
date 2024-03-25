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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.RDGCType;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DetailGCHelper {

    public static void enableDetailGC(final VersionGarbageCollector vgc) throws IllegalAccessException {
        if (vgc != null) {
            writeField(vgc, "detailedGCEnabled", true, true);
        }
    }

    public static void disableDetailGC(final VersionGarbageCollector vgc) throws IllegalAccessException {
        if (vgc != null) {
            writeField(vgc, "detailedGCEnabled", false, true);
        }
    }
    public static void enableDetailGCDryRun(final VersionGarbageCollector vgc) throws IllegalAccessException {
        if (vgc != null) {
            writeField(vgc, "isDetailedGCDryRun", true, true);
        }
    }

    public static void disableDetailGCDryRun(final VersionGarbageCollector vgc) throws IllegalAccessException {
        if (vgc != null) {
            writeField(vgc, "isDetailedGCDryRun", false, true);
        }
    }

    public static RevisionVector mergedBranchCommit(final NodeStore store, final Consumer<NodeBuilder> buildFunction) throws Exception {
        return build(store, true, true, buildFunction);
    }

    public static RevisionVector unmergedBranchCommit(final NodeStore store, final Consumer<NodeBuilder> buildFunction) throws Exception {
        RevisionVector result = build(store, true, false, buildFunction);
        assertTrue(result.isBranch());
        return result;
    }

    public static void assertBranchRevisionRemovedFromAllDocuments(
            final DocumentNodeStore store, final RevisionVector br,
            final String... exceptIds) {
        assertTrue(br.isBranch());
        Revision br1 = br.getRevision(1);
        assert br1 != null;
        Revision r1 = br1.asTrunkRevision();
        Set<String> except = Set.of(exceptIds);
        for (NodeDocument nd : Utils.getAllDocuments(store.getDocumentStore())) {
            if (nd.getId().equals(Utils.getIdFromPath(Path.ROOT))
                    || except.contains(nd.getId())) {
                // skip root and the provided ids
                continue;
            }
            NodeDocument target = new NodeDocument(store.getDocumentStore());
            nd.deepCopy(target);
            // ignore the _revisions entry, as we cannot always cleanup everything in there
            target.remove(NodeDocument.REVISIONS);
            for (Entry<String, Object> e : target.data.entrySet()) {
                String k = e.getKey();
                final boolean internal = k.startsWith("_");
                final boolean dgcSupportsInternalPropCleanup = (VersionGarbageCollector.revisionDetailedGcType != RDGCType.KEEP_ONE_CLEANUP_USER_PROPERTIES_ONLY_MODE);
                if (internal && !dgcSupportsInternalPropCleanup) {
                    // skip
                    continue;
                }
                assertFalse("document not cleaned up for prop " + k + " : " + e,
                        e.toString().contains(r1.toString()));
            }
        }
    }

    public static void assertBranchRevisionNotRemovedFromAllDocuments(final DocumentNodeStore store, final RevisionVector br) {
        assertTrue(br.isBranch());
        Revision br1 = br.getRevision(1);
        assert br1 != null;
        Revision r1 = br1.asTrunkRevision();
        for (NodeDocument nd : Utils.getAllDocuments(store.getDocumentStore())) {
            if (Objects.equals(nd.getId(), "0:/")) {
                NodeDocument target = new NodeDocument(store.getDocumentStore());
                nd.deepCopy(target);
                assertTrue("document not cleaned up: " + nd.asString(), nd.asString().contains(r1.toString()));
            }
        }
    }

    static RevisionVector build(final NodeStore store, final boolean persistToBranch, final boolean merge,
                                        final Consumer<NodeBuilder> buildFunction) throws Exception {
        if (!persistToBranch && !merge) {
            throw new IllegalArgumentException("must either persistToBranch or merge");
        }
        NodeBuilder b = store.getRoot().builder();
        buildFunction.accept(b);
        RevisionVector result = null;
        if (persistToBranch) {
            DocumentRootBuilder drb = (DocumentRootBuilder) b;
            drb.persist();
            DocumentNodeState ns = (DocumentNodeState) drb.getNodeState();
            result = ns.getLastRevision();
        }
        if (merge) {
            DocumentNodeState dns = (DocumentNodeState) merge(store, b);
            result = dns.getLastRevision();
        }
        return result;
    }

    private static NodeState merge(final NodeStore store, final NodeBuilder builder) throws Exception {
        return store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
