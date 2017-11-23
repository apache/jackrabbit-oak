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

package org.apache.jackrabbit.oak.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * Editor implementation which stores the property index NodeState data in a different
 * SegmentNodeStore used solely for property index storage purpose
 */
public class SegmentPropertyIndexEditorProvider implements IndexEditorProvider, Closeable {

    private MountInfoProvider mountInfoProvider = Mounts.defaultMountInfoProvider();

    private final File indexStoreDir;
    private NodeBuilder rootBuilder;
    private NodeStoreFixture fixture;

    public SegmentPropertyIndexEditorProvider(File storeDir) {
        this.indexStoreDir = storeDir;
    }

    @CheckForNull
    @Override
    public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition,
                                 @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
            throws CommitFailedException {
        if (!PropertyIndexEditorProvider.TYPE.equals(type)) {
            return null;
        }
        IndexingContext idxCtx = ((ContextAwareCallback) callback).getIndexingContext();
        String indexPath = idxCtx.getIndexPath();

        PropertyIndexEditorProvider pie = new PropertyIndexEditorProvider();
        pie.with(mountInfoProvider);

        NodeBuilder idxb = definition;
        if (idxCtx.isReindexing()) {
            //In case of reindex use the NodeBuilder from SegmentNodeStore instead of default one
            idxb = createNewBuilder(indexPath, definition);
        }

        return pie.getIndexEditor(type, idxb, root, callback);
    }

    @Override
    public void close() throws IOException {
        if (rootBuilder != null) {
            try {
                fixture.getStore().merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            } catch (CommitFailedException e) {
                throw new IOException(e);
            }
            fixture.close();
        }
    }

    private NodeBuilder createNewBuilder(String indexPath, NodeBuilder definition) {
        String idxNodeName = PathUtils.getName(indexPath);
        String idxParentPath = PathUtils.getParentPath(indexPath);
        NodeBuilder newIdxBuilder = child(getRootBuilder(), idxParentPath);
        NodeState nodeState = cloneVisibleState(definition.getNodeState());
        newIdxBuilder.setChildNode(idxNodeName, nodeState);
        return newIdxBuilder.child(idxNodeName);
    }

    private NodeBuilder getRootBuilder() {
        if (rootBuilder == null) {
            rootBuilder = createRootBuilder();
        }
        return rootBuilder;
    }

    private NodeBuilder createRootBuilder() {
        try {
            indexStoreDir.mkdirs();
            fixture = NodeStoreFixtureProvider.create(createSegmentOptions(indexStoreDir), false);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Segment store at " + indexStoreDir.getAbsolutePath(), e);
        }
        return fixture.getStore().getRoot().builder();
    }

    private static Options createSegmentOptions(File storePath) throws IOException {
        OptionParser parser = new OptionParser();
        Options opts = new Options().withDisableSystemExit();
        opts.parseAndConfigure(parser, new String[] {storePath.getAbsolutePath()});
        return opts;
    }

    private static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }

    public SegmentPropertyIndexEditorProvider with(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
        return this;
    }

    private static NodeState cloneVisibleState(NodeState state){
        NodeBuilder builder = EMPTY_NODE.builder();
        new ApplyVisibleDiff(builder).apply(state);
        return builder.getNodeState();
    }

    private static class ApplyVisibleDiff extends ApplyDiff {
        public ApplyVisibleDiff(NodeBuilder builder) {
            super(builder);
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (NodeStateUtils.isHidden(name)){
                return true;
            }
            return after.compareAgainstBaseState(
                    EMPTY_NODE, new ApplyVisibleDiff(builder.child(name)));
        }
    }
}
