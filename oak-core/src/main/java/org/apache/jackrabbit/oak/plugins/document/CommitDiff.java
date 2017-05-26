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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingHandler;
import org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

/**
 * Implementation of a {@link NodeStateDiff}, which translates the diffs into
 * {@link UpdateOp}s of a commit.
 */
class CommitDiff implements NodeStateDiff {

    private final DocumentNodeStore store;

    private final Commit commit;

    private final JsopBuilder builder;

    private final BlobSerializer blobs;

    private final BundlingHandler bundlingHandler;

    CommitDiff(@Nonnull DocumentNodeStore store, @Nonnull Commit commit,
               @Nonnull BlobSerializer blobs) {
        this(checkNotNull(store), checkNotNull(commit), store.getBundlingConfigHandler().newBundlingHandler(),
                new JsopBuilder(), checkNotNull(blobs));
    }

    private CommitDiff(DocumentNodeStore store, Commit commit, BundlingHandler bundlingHandler,
               JsopBuilder builder, BlobSerializer blobs) {
        this.store = store;
        this.commit = commit;
        this.bundlingHandler = bundlingHandler;
        this.builder = builder;
        this.blobs = blobs;
        performBundlingRelatedOperations();
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        setProperty(after);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        setProperty(after);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        commit.updateProperty(bundlingHandler.getRootBundlePath(), bundlingHandler.getPropertyPath(before.getName()), null);
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        BundlingHandler child = bundlingHandler.childAdded(name, after);
        if (child.isBundlingRoot()) {
            commit.addNode(new DocumentNodeState(store, child.getRootBundlePath(),
                    new RevisionVector(commit.getRevision())));
        }
        setOrTouchChildrenFlag(child);
        return after.compareAgainstBaseState(EMPTY_NODE,
                new CommitDiff(store, commit, child, builder, blobs));
    }

    @Override
    public boolean childNodeChanged(String name,
                                    NodeState before,
                                    NodeState after) {
        //TODO [bundling] Handle change of primaryType. Current approach would work
        //but if bundling was enabled for previous nodetype its "side effect"
        //would still impact even though new nodetype does not have bundling enabled
        BundlingHandler child = bundlingHandler.childChanged(name, before, after);
        return after.compareAgainstBaseState(before,
                new CommitDiff(store, commit, child, builder, blobs));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        BundlingHandler child = bundlingHandler.childDeleted(name, before);
        if (child.isBundlingRoot()) {
            commit.removeNode(child.getRootBundlePath(), before);
        }
        setOrTouchChildrenFlag(child);
        return MISSING_NODE.compareAgainstBaseState(before,
                new CommitDiff(store, commit, child, builder, blobs));
    }

    //----------------------------< internal >----------------------------------

    private void performBundlingRelatedOperations() {
        setMetaProperties();
        informCommitAboutBundledNodes();
        removeRemovedProps();
    }

    private void setMetaProperties() {
        for (PropertyState ps : bundlingHandler.getMetaProps()){
            setProperty(ps);
        }
    }

    private void informCommitAboutBundledNodes() {
        if (bundlingHandler.isBundledNode()){
            commit.addBundledNode(bundlingHandler.getNodeFullPath(), bundlingHandler.getRootBundlePath());
        }
    }

    private void removeRemovedProps() {
        for (String propName : bundlingHandler.getRemovedProps()){
            commit.updateProperty(bundlingHandler.getRootBundlePath(),
                    bundlingHandler.getPropertyPath(propName), null);
        }
    }

    private void setOrTouchChildrenFlag(BundlingHandler child) {
        //Add hasChildren marker for bundling case
        String propName = null;
        if (child.isBundledNode()){
            //1. Child is a bundled node. In that case current node would be part
            //   same NodeDocument in which the child would be saved
            propName = DocumentBundlor.META_PROP_BUNDLED_CHILD;
        } else if (bundlingHandler.isBundledNode()){
            //2. Child is a non bundled node but current node was bundled. This would
            //   be the case where child node is not covered by bundling pattern. In
            //   that case also add marker to current node
            //   For case when current node is bundled  but is bundling root
            //   this info is already captured in _hasChildren flag
            propName = DocumentBundlor.META_PROP_NON_BUNDLED_CHILD;
        }

        //Retouch the property if already present to enable
        //hierarchy conflict detection
        if (propName != null){
            setProperty(createProperty(propName, Boolean.TRUE));
        }
    }

    private void setProperty(PropertyState property) {
        builder.resetWriter();
        JsonSerializer serializer = new JsonSerializer(builder, blobs);
        serializer.serialize(property);
        commit.updateProperty(bundlingHandler.getRootBundlePath(), bundlingHandler.getPropertyPath(property.getName()),
                 serializer.toString());
        if ((property.getType() == Type.BINARY)
                || (property.getType() == Type.BINARIES)) {
            this.commit.markNodeHavingBinary(bundlingHandler.getRootBundlePath());
        }
    }
}
