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
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.BlobSerializer;
import org.apache.jackrabbit.oak.kernel.JsonSerializer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

/**
 * Implementation of a {@link NodeStateDiff}, which translates the diffs into
 * {@link UpdateOp}s of a commit.
 */
class CommitDiff implements NodeStateDiff {

    private final DocumentNodeStore store;

    private final Commit commit;

    private final String path;

    private final JsopBuilder builder;

    private final BlobSerializer blobs;

    CommitDiff(@Nonnull DocumentNodeStore store, @Nonnull Commit commit,
               @Nonnull BlobSerializer blobs) {
        this(checkNotNull(store), checkNotNull(commit), "/",
                new JsopBuilder(), checkNotNull(blobs));
    }

    private CommitDiff(DocumentNodeStore store, Commit commit, String path,
               JsopBuilder builder, BlobSerializer blobs) {
        this.store = store;
        this.commit = commit;
        this.path = path;
        this.builder = builder;
        this.blobs = blobs;
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
        commit.updateProperty(path, before.getName(), null);
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        String p = PathUtils.concat(path, name);
        commit.addNode(new DocumentNodeState(store, p, commit.getRevision()));
        return after.compareAgainstBaseState(EMPTY_NODE,
                new CommitDiff(store, commit, p, builder, blobs));
    }

    @Override
    public boolean childNodeChanged(String name,
                                    NodeState before,
                                    NodeState after) {
        String p = PathUtils.concat(path, name);
        return after.compareAgainstBaseState(before,
                new CommitDiff(store, commit, p, builder, blobs));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        String p = PathUtils.concat(path, name);
        commit.removeNode(p);
        return MISSING_NODE.compareAgainstBaseState(before,
                new CommitDiff(store, commit, p, builder, blobs));
    }

    //----------------------------< internal >----------------------------------

    private void setProperty(PropertyState property) {
        builder.resetWriter();
        JsonSerializer serializer = new JsonSerializer(builder, blobs);
        serializer.serialize(property);
        commit.updateProperty(path, property.getName(), serializer.toString());
        if ((property.getType() == Type.BINARY) 
                || (property.getType() == Type.BINARIES)) {
            this.commit.markNodeHavingBinary(this.path);
        }
    }
}
