package org.apache.jackrabbit.oak.index.indexer.document.tree.nodeStore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TreeNodeStore implements NodeStore {
    
    private final TreeStore treeStore;
    
    private TreeNodeStore(TreeStore treeStore) {
        this.treeStore = treeStore;
    }
    
    public static NodeStore create(String storeArg) {
        String dir = storeArg.substring("tree:".length());
        File directory = new File(dir);
        MemoryBlobStore blobStore = new MemoryBlobStore();
        NodeStateEntryReader entryReader = new NodeStateEntryReader(blobStore);
        TreeStore treeStore = new TreeStore(directory, entryReader);
        return new TreeNodeStore(treeStore);
    }

    @Override
    public @NotNull NodeState getRoot() {
        return treeStore.getNodeStateEntry("/").getNodeState();
    }

    public String reindexNodeCounter() throws CommitFailedException {
        NodeState before = EmptyNodeState.MISSING_NODE;
        NodeBuilder builder = getRoot().getChildNode("oak:index").getChildNode("counter").builder();
        Editor editor = new NodeCounterEditorProvider().getIndexEditor("counter",
                builder, getRoot(), new IndexUpdateCallback() {
                    @Override
                    public void indexUpdate() throws CommitFailedException {
                        // nothing to do
                    }
        });
        CommitFailedException exception = EditorDiff.process(VisibleEditor.wrap(editor), before, getRoot());
        if (exception != null) {
            throw exception;
        }
        JsopBuilder json = new JsopBuilder();
        new JsonSerializer(json, "{}", new Base64BlobSerializer()).serialize(builder.getNodeState());
        return json.toString();
    }

    @Override
    public @NotNull String checkpoint(long lifetime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull Map<String, String> checkpointInfo(@NotNull String checkpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull Iterable<String> checkpoints() {
        return Collections.emptyList();
    }

    @Override
    public @NotNull Blob createBlob(InputStream inputStream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable Blob getBlob(@NotNull String reference) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull NodeState merge(
            @NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
            @NotNull CommitInfo info) throws CommitFailedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull NodeState rebase(@NotNull NodeBuilder builder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean release(@NotNull String checkpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable NodeState retrieve(@NotNull String checkpoint) {
        throw new UnsupportedOperationException();
    }

}
