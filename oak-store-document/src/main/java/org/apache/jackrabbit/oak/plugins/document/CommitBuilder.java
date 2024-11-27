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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.Maps;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * A builder for a commit, translating modifications into {@link UpdateOp}s.
 */
class CommitBuilder {

    /** A marker revision when the commit is initially built */
    static final Revision PSEUDO_COMMIT_REVISION = new Revision(Long.MIN_VALUE, 0, 0);

    private final DocumentNodeStore nodeStore;
    private final Revision revision;
    private final RevisionVector baseRevision;
    private RevisionVector startRevisions = new RevisionVector();
    private final Map<Path, UpdateOp> operations = new LinkedHashMap<>();

    private final Set<Path> addedNodes = new HashSet<>();
    private final Set<Path> removedNodes = new HashSet<>();

    /** Set of all nodes which have binary properties. **/
    private final Set<Path> nodesWithBinaries = new HashSet<>();
    private final Map<Path, Path> bundledNodes = new HashMap<>();

    /**
     * Creates a new builder with a pseudo commit revision. Building the commit
     * must be done by calling {@link #build(Revision)}.
     *
     * @param nodeStore the node store.
     * @param baseRevision the base revision if available.
     */
    CommitBuilder(@NotNull DocumentNodeStore nodeStore,
                  @Nullable RevisionVector baseRevision) {
        this(nodeStore, PSEUDO_COMMIT_REVISION, baseRevision);
    }

    /**
     * Creates a new builder with the given commit {@code revision}.
     *
     * @param nodeStore the node store.
     * @param revision the commit revision.
     * @param baseRevision the base revision of the commit or {@code null} if
     *          none is set.
     */
    CommitBuilder(@NotNull DocumentNodeStore nodeStore,
                  @NotNull Revision revision,
                  @Nullable RevisionVector baseRevision) {
        this.nodeStore = requireNonNull(nodeStore);
        this.revision = requireNonNull(revision);
        this.baseRevision = baseRevision;
    }

    /**
     * @return the commit revision.
     */
    @NotNull
    Revision getRevision() {
        return revision;
    }

    /**
     * @return the base revision or {@code null} if none is set.
     */
    @Nullable
    RevisionVector getBaseRevision() {
        return baseRevision;
    }

    /**
     * Add a node to the commit with the given path.
     *
     * @param path the path of the node to add.
     * @return {@code this} builder.
     */
    @NotNull
    CommitBuilder addNode(@NotNull Path path) {
        addNode(new DocumentNodeState(nodeStore, path, new RevisionVector(revision)));
        return this;
    }

    /**
     * Add a the given node and its properties to the commit.
     *
     * @param node the node state to add.
     * @return {@code this} builder.
     * @throws DocumentStoreException if there's already a modification for
     *      a node at the given {@code path} in this commit builder.
     */
    @NotNull
    CommitBuilder addNode(@NotNull DocumentNodeState node)
            throws DocumentStoreException {
        requireNonNull(node);

        Path path = node.getPath();

        if (Utils.isNodeNameLong(path, nodeStore.getDocumentStore().getNodeNameLimit())) {
            throw new DocumentStoreException("Node name is too long: " + path);
        }

        UpdateOp op = node.asOperation(revision);
        if (operations.containsKey(path)) {
            String msg = "Node already added: " + path;
            throw new DocumentStoreException(msg);
        }
        if (isBranchCommit()) {
            NodeDocument.setBranchCommit(op, revision);
        }
        operations.put(path, op);
        addedNodes.add(path);
        return this;
    }

    /**
     * Instructs the commit builder that the bundling root of the node at
     * {@code path} is at {@code bundlingRootPath}.
     *
     * @param path the path of a node.
     * @param bundlingRootPath the bundling root for the node.
     * @return {@code this} builder.
     */
    @NotNull
    CommitBuilder addBundledNode(@NotNull Path path,
                                 @NotNull Path bundlingRootPath) {
        requireNonNull(path);
        requireNonNull(bundlingRootPath);

        bundledNodes.put(path, bundlingRootPath);
        return this;
    }

    /**
     * Removes a node in this commit.
     *
     * @param path the path of the node to remove.
     * @param state the node state representing the node to remove.
     * @return {@code this} builder.
     * @throws DocumentStoreException if there's already a modification for
     *      a node at the given {@code path} in this commit builder.
     */
    @NotNull
    CommitBuilder removeNode(@NotNull Path path,
                             @NotNull NodeState state)
            throws DocumentStoreException {
        requireNonNull(path);
        requireNonNull(state);

        if (operations.containsKey(path)) {
            String msg = "Node already removed: " + path;
            throw new DocumentStoreException(msg);
        }
        removedNodes.add(path);
        UpdateOp op = getUpdateOperationForNode(path);
        op.setDelete(true);
        NodeDocument.setDeleted(op, revision, true);
        for (PropertyState p : state.getProperties()) {
            updateProperty(path, p.getName(), null);
        }
        return this;
    }

    /**
     * Updates a property to a given value.
     *
     * @param path the path of the node.
     * @param propertyName the name of the property.
     * @param value the value of the property.
     * @return {@code this} builder.
     */
    @NotNull
    CommitBuilder updateProperty(@NotNull Path path,
                                 @NotNull String propertyName,
                                 @Nullable String value) {
        requireNonNull(path);
        requireNonNull(propertyName);

        UpdateOp op = getUpdateOperationForNode(path);
        String key = Utils.escapePropertyName(propertyName);
        op.setMapEntry(key, revision, value);
        return this;
    }

    /**
     * Instructs the commit builder that the node at the given {@code path} has
     * a reference to a binary.
     *
     * @param path the path of the node.
     * @return {@code this} builder.
     */
    @NotNull
    CommitBuilder markNodeHavingBinary(@NotNull Path path) {
        requireNonNull(path);

        nodesWithBinaries.add(path);
        return this;
    }

    /**
     * Sets the start revisions of known clusterIds on this commit builder.
     *
     * @param startRevisions the start revisions derived from the start time
     *          in the clusterNodes entries.
     * @return {@code this} builder.
     */
    @NotNull
    CommitBuilder withStartRevisions(@NotNull RevisionVector startRevisions) {
        this.startRevisions = requireNonNull(startRevisions);
        return this;
    }

    /**
     * Builds the commit with the modifications.
     *
     * @return {@code this} builder.
     * @throws IllegalStateException if this builder was created without an
     *      explicit commit revision and {@link #build(Revision)} should have
     *      been called instead.
     */
    @NotNull
    Commit build() {
        if (PSEUDO_COMMIT_REVISION.equals(revision)) {
            String msg = "Cannot build a commit with a pseudo commit revision";
            throw new IllegalStateException(msg);
        }
        return new Commit(nodeStore, revision, baseRevision, startRevisions,
                operations, addedNodes, removedNodes, nodesWithBinaries,
                bundledNodes);
    }

    /**
     * Builds the commit with the modifications and the given commit revision.
     *
     * @param revision the commit revision.
     * @return {@code this} builder.
     */
    @NotNull
    Commit build(@NotNull Revision revision) {
        requireNonNull(revision);

        Revision from = this.revision;
        Map<Path, UpdateOp> operations = Maps.transformValues(
                this.operations, op -> rewrite(op, from, revision));
        return new Commit(nodeStore, revision, baseRevision, startRevisions,
                operations, addedNodes, removedNodes, nodesWithBinaries,
                bundledNodes);
    }

    /**
     * Returns the number of operations currently recorded by this commit
     * builder.
     *
     * @return the number of operations.
     */
    int getNumOperations() {
        return operations.size();
    }

    //-------------------------< internal >-------------------------------------

    private UpdateOp getUpdateOperationForNode(Path path) {
        UpdateOp op = operations.get(path);
        if (op == null) {
            op = createUpdateOp(path, revision, isBranchCommit());
            operations.put(path, op);
        }
        return op;
    }

    private static UpdateOp createUpdateOp(Path path,
                                           Revision revision,
                                           boolean isBranch) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(id, false);
        NodeDocument.setModified(op, revision);
        if (isBranch) {
            NodeDocument.setBranchCommit(op, revision);
        }
        return op;
    }

    /**
     * @return {@code true} if this is a branch commit.
     */
    private boolean isBranchCommit() {
        return baseRevision != null && baseRevision.isBranch();
    }

    private static UpdateOp rewrite(UpdateOp up, Revision from, Revision to) {
        Map<UpdateOp.Key, UpdateOp.Operation> changes = new HashMap<>();
        for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : up.getChanges().entrySet()) {
            UpdateOp.Key k = entry.getKey();
            UpdateOp.Operation op = entry.getValue();
            if (from.equals(k.getRevision())) {
                k = new UpdateOp.Key(k.getName(), to);
            } else if (NodeDocument.MODIFIED_IN_SECS.equals(k.getName())) {
                op = new UpdateOp.Operation(op.type, NodeDocument.getModifiedInSecs(to.getTimestamp()));
            }
            changes.put(k, op);
        }

        return new UpdateOp(up.getId(), up.isNew(), up.isDelete(), changes, null);
    }
}
