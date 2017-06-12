/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.cow;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.CopyOnWriteStoreMBean;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * <p>The copy-on-write (COW) node store implementation allows to temporarily
 * switch the repository into the "testing" mode, in which all the changes are
 * stored in a volatile storage, namely the MemoryNodeStore. After switching
 * back to the "production" mode, the test changes should be dropped.</p>
 *
 * <p>If the CoW is enabled, a special :cow=true property will be set on the
 * root node returned by getRoot(). It's being used in the merge() to decide
 * which store be modified. Removing this property will result in merging
 * changes to the main node store, even in the CoW mode.</p>
 *
 * <p>The checkpoint support is provided by the {@link BranchNodeStore} class.
 * All the existing checkpoints are still available in the CoW mode (until they
 * expire). New checkpoints are only created in the MemoryNodeStore.</p>
 *
 * <p>Known limitations:</p>
 *
 * <ul>
 *     <li>turning the CoW mode on and off requires cleaning up the
 *     <a href="https://jackrabbit.apache.org/oak/docs/query/lucene.html#copy-on-read">lucene
 *     indexing cache</a>,</li>
 *     <li>switching the CoW mode may result in repository inconsistencies
 *     (eg. if two merges belongs to the same logical commit sequence),</li>
 *     <li>in the CoW mode the changes are stored in MemoryNodeStore, so it
 *     shouldn't be enabled for too long (otherwise it may exhaust the heap).</li>
 * </ul>
 */
public class COWNodeStore implements NodeStore, Observable {

    private final List<Observer> observers = new CopyOnWriteArrayList<>();

    private final NodeStore store;

    private volatile BranchNodeStore branchStore;

    public COWNodeStore(NodeStore store) {
        this.store = store;
    }

    public void enableCopyOnWrite() throws CommitFailedException {
        BranchNodeStore branchStore = new BranchNodeStore(store);

        NodeBuilder b = branchStore.getRoot().builder();
        b.setProperty(":cow", true);
        branchStore.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        branchStore.addObserver((root, info) -> observers.stream().forEach(o -> o.contentChanged(root, info)));
        this.branchStore = branchStore;
    }

    public void disableCopyOnWrite() {
        BranchNodeStore branchStore = this.branchStore;
        this.branchStore = null;
        branchStore.dispose();
    }

    private NodeStore getNodeStore() {
        NodeStore s = branchStore;
        if (s == null) {
            s = store;
        }
        return s;
    }

    private NodeStore getNodeStore(NodeBuilder builder) {
        if (builder.hasProperty(":cow")) {
            NodeStore s = branchStore;
            if (s == null) {
                throw new IllegalStateException("Node store for this builder is no longer available");
            } else {
                return s;
            }
        } else {
            return store;
        }
    }

    @Override
    public Closeable addObserver(Observer observer) {
        observer.contentChanged(getRoot(), CommitInfo.EMPTY_EXTERNAL);
        observers.add(observer);
        return () -> observers.remove(observer);
    }

    @Nonnull
    @Override
    public NodeState getRoot() {
        return getNodeStore().getRoot();
    }

    @Nonnull
    @Override
    public NodeState merge(@Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook, @Nonnull CommitInfo info) throws CommitFailedException {
        return getNodeStore(builder).merge(builder, commitHook, info);
    }

    @Nonnull
    @Override
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        return getNodeStore(builder).rebase(builder);
    }

    @Override
    public NodeState reset(@Nonnull NodeBuilder builder) {
        return getNodeStore(builder).reset(builder);
    }

    @Nonnull
    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        return getNodeStore().createBlob(inputStream);
    }

    @Override
    public Blob getBlob(@Nonnull String reference) {
        return getNodeStore().getBlob(reference);
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
        return getNodeStore().checkpoint(lifetime, properties);
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime) {
        return getNodeStore().checkpoint(lifetime);
    }

    @Nonnull
    @Override
    public Map<String, String> checkpointInfo(@Nonnull String checkpoint) {
        return getNodeStore().checkpointInfo(checkpoint);
    }

    @Nonnull
    @Override
    public Iterable<String> checkpoints() {
        return getNodeStore().checkpoints();
    }

    @Override
    public NodeState retrieve(@Nonnull String checkpoint) {
        return getNodeStore().retrieve(checkpoint);
    }

    @Override
    public boolean release(@Nonnull String checkpoint) {
        return getNodeStore().release(checkpoint);
    }

    class MBeanImpl implements CopyOnWriteStoreMBean {

        @Override
        public String enableCopyOnWrite() {
            try {
                COWNodeStore.this.enableCopyOnWrite();
            } catch (CommitFailedException e) {
                return "can't enable the copy on write: " + e.getMessage();
            }
            return "success";
        }

        @Override
        public String disableCopyOnWrite() {
            COWNodeStore.this.disableCopyOnWrite();
            return "success";
        }

        @Override
        public String getStatus() {
            return branchStore == null ? "disabled" : "enabled";
        }
    }


}
