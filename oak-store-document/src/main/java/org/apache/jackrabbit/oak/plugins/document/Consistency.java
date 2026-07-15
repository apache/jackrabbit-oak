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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;

/**
 * Consistency check on a {@code NodeDocument}.
 */
public class Consistency {

    private final DocumentNodeState root;

    private final NodeDocument document;

    /**
     * Creates a new consistency check for the given {@code NodeDocument}.
     *
     * @param root the current root node state to use for the check.
     * @param document the document to check.
     */
    public Consistency(@NotNull DocumentNodeState root,
                       @NotNull NodeDocument document) {
        this.root = requireNonNull(root);
        this.document = requireNonNull(document);
    }

    public void check(@NotNull DocumentNodeStore ns,
                      @NotNull Result collector) {
        requireNonNull(ns);
        requireNonNull(collector);

        NodeState traversedState = root;
        AbstractDocumentNodeState existingAncestorOrSelf = root;
        for (String name : document.getPath().elements()) {
            traversedState = traversedState.getChildNode(name);
            if (traversedState instanceof DocumentNodeState) {
                existingAncestorOrSelf = (DocumentNodeState) traversedState;
            }
        }
        DocumentNodeState dns = document.getNodeAtRevision(ns, root.getRootRevision(), null);
        if (traversedState.exists()) {
            if (dns != null) {
                if (!equalProperties(dns, traversedState)) {
                    // both exist, but they are not equal
                    identifyChanges(ns, traversedState, collector);
                }
            } else {
                // exists when traversing to the node, getting node directly
                // from document returned null
                // document must have _deleted set to true after some _lastRev
                identifyChanges(ns, traversedState, collector);
            }
        } else {
            if (dns != null) {
                // does not exist when traversing to node, but getting node
                // directly returns it. node is orphaned
                identifyChanges(ns, existingAncestorOrSelf, collector);
            }
        }
    }

    /**
     * Callback interface for result of a consistency check.
     */
    public interface Result {

        /**
         * Called when the consistency check identified an inconsistency. The
         * reported {@code Revision} identifies the associated change with
         * the inconsistency.
         *
         * @param revision the revision of the change.
         */
        void inconsistent(Revision revision);
    }

    /**
     * Compare the properties of both {@link NodeState}s and return {@code true}
     * if they are equal, otherwise {@code false}.
     *
     * @param a a node state
     * @param b another node state
     * @return {@code true} if properties on both {@code NodeState}s are equal,
     *      {@code false} otherwise.
     */
    private static boolean equalProperties(NodeState a, NodeState b) {
        if (a.getPropertyCount() != b.getPropertyCount()) {
            return false;
        }
        for (PropertyState property : a.getProperties()) {
            if (!property.equals(b.getProperty(property.getName()))) {
                return false;
            }
        }
        return true;
    }

    private void identifyChanges(@NotNull RevisionContext context,
                                 @NotNull NodeState state,
                                 @NotNull Result result) {
        if (state instanceof DocumentNodeState) {
            DocumentNodeState traversedState = (DocumentNodeState) state;
            // must not have any committed changes between lastRevision on
            // traversed node state and root revision on this document
            RevisionVector lastRevision = traversedState.getLastRevision();
            RevisionVector rootRevision = traversedState.getRootRevision();
            for (Revision r : document.getLocalCommitRoot().keySet()) {
                if (!rootRevision.isRevisionNewer(r)
                        && lastRevision.isRevisionNewer(r)
                        && Utils.isCommitted(context.getCommitValue(r, document))) {
                    // committed change after lastRevision, but not newer
                    // than root revision
                    result.inconsistent(r);
                }
            }
        }
    }
}
