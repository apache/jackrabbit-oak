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
package org.apache.jackrabbit.oak.kernel;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.ByteStreams;

public class NodeStoreKernel implements MicroKernel {

    private final NodeStore store;

    private final Map<String, NodeState> revisions = newLinkedHashMap();

    private final Map<String, NodeBuilder> branches = newLinkedHashMap();

    private final Map<String, Blob> blobs = newConcurrentMap();

    private String head;

    public NodeStoreKernel(NodeStore store) {
        this.store = store;
        this.head = UUID.randomUUID().toString();
        revisions.put(head, store.getRoot());
    }

    private synchronized NodeState getRoot(String revision)
            throws MicroKernelException {
        if (revision == null) {
            revision = head;
        }

        NodeState root = revisions.get(revision);
        if (root != null) {
            return root;
        }

        NodeBuilder builder = branches.get(revision);
        if (builder != null) {
            return builder.getNodeState();
        }

        throw new MicroKernelException("Revision not found: " + revision);
    }

    private NodeState getNode(String revision, String path) {
        NodeState node = getRoot(revision);
        for (String element : PathUtils.elements(path)) {
            node = node.getChildNode(element);
        }
        return node;
    }

    private void applyJsop(NodeBuilder builder, String path, String jsonDiff)
            throws MicroKernelException {
        for (String element : PathUtils.elements(path)) {
            builder = builder.getChildNode(element);
        }
        if (builder.exists()) {
            applyJsop(builder, jsonDiff);
        } else {
            throw new MicroKernelException("Path not found: " + path);
        }
    }

    private void applyJsop(NodeBuilder builder, String jsonDiff) {
        JsopTokenizer tokenizer = new JsopTokenizer(jsonDiff);
        int token = tokenizer.read();
        while (token != JsopReader.END) {
            String path = tokenizer.readString();
            String name = getName(path);
            switch (token) {
            case '+':
                tokenizer.read(':');
                tokenizer.read('{');
                NodeBuilder parent = getNode(builder, getParentPath(path));
                if (builder.hasChildNode(name)) {
                    throw new MicroKernelException(
                            "Node already exists: " + path);
                }
                addNode(parent.setChildNode(name), tokenizer);
                break;
            case '-':
                getNode(builder, path).remove();
                break;
            case '^':
                tokenizer.read(':');
                NodeBuilder node = getNode(builder, getParentPath(path));
                switch (tokenizer.read()) {
                case JsopReader.NULL:
                    node.removeProperty(name);
                    break;
                case JsopReader.FALSE:
                    node.setProperty(name, Boolean.FALSE);
                    break;
                case JsopReader.TRUE:
                    node.setProperty(name, Boolean.TRUE);
                    break;
                case JsopReader.STRING:
                    node.setProperty(name, tokenizer.getToken());
                    break;
                case JsopReader.NUMBER:
                    String value = tokenizer.getToken();
                    try {
                        node.setProperty(name, Long.parseLong(value));
                    } catch (NumberFormatException e) {
                        node.setProperty(name, Double.parseDouble(value));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
                break;
            case '>':
                throw new UnsupportedOperationException();
            case '*':
                throw new UnsupportedOperationException();
            default:
                throw new MicroKernelException(
                        "Unexpected token: " + tokenizer.getEscapedToken());
            }
            token = tokenizer.read();
        }
    }

    private NodeBuilder getNode(NodeBuilder builder, String path) {
        for (String element : PathUtils.elements(path)) {
            builder = builder.getChildNode(element);
        }
        if (builder.exists()) {
            return builder;
        } else {
            throw new MicroKernelException("Path not found: " + path);
        }
    }

    private void addNode(NodeBuilder builder, JsopTokenizer tokenizer)
            throws MicroKernelException {
        if (tokenizer.matches('}')) {
            return;
        }
        do {
            String name = tokenizer.readString();
            tokenizer.read(':');
            switch (tokenizer.read()) {
            case '{':
                NodeBuilder child = builder.setChildNode(name);
                addNode(child, tokenizer);
                break;
            case '[':
                // FIXME: proper array parsing with support for more types
                List<Long> array = newArrayList();
                while (tokenizer.matches(JsopReader.NUMBER)) {
                    array.add(Long.parseLong(tokenizer.getToken()));
                    tokenizer.matches(',');
                }
                tokenizer.read(']');
                builder.setProperty(name, array, Type.LONGS);
                break;
            case JsopReader.FALSE:
                builder.setProperty(name, Boolean.FALSE);
                break;
            case JsopReader.TRUE:
                builder.setProperty(name, Boolean.TRUE);
                break;
            case JsopReader.NUMBER:
                String value = tokenizer.getToken();
                try {
                    builder.setProperty(name, Long.parseLong(value));
                } catch (NumberFormatException e) {
                    builder.setProperty(name, Double.parseDouble(value));
                }
                break;
            case JsopReader.STRING:
                builder.setProperty(name, tokenizer.getToken());
                break;
            default:
                throw new MicroKernelException(
                        "Unexpected token: " + tokenizer.getEscapedToken());
            }
        } while (tokenizer.matches(','));
        tokenizer.read('}');
    }

    @Override
    public synchronized String getHeadRevision() {
        NodeState root = store.getRoot();
        if (!root.equals(revisions.get(head))) {
            head = UUID.randomUUID().toString();
            revisions.put(head, root);
            notifyAll();
        }
        return head;
    }

    @Override
    public String checkpoint(long lifetime) {
        return getHeadRevision();
    }

    @Override
    public synchronized String getRevisionHistory(
            long since, int maxEntries, String path)
            throws MicroKernelException {
        JsopBuilder json = new JsopBuilder();
        json.array();
        int count = 0;
        for (String revision : revisions.keySet()) {
            if (count++ > maxEntries) {
                break;
            }
            json.object();
            json.key("id").value(revision);
            json.endObject();
        }
        json.endArray();
        return json.toString();
    }

    @Override
    public synchronized String waitForCommit(
            String oldHeadRevisionId, long timeout)
            throws MicroKernelException, InterruptedException {
        long stop = System.currentTimeMillis() + timeout;
        while (head.equals(oldHeadRevisionId) && timeout > 0) {
            wait(timeout);
            timeout = stop - System.currentTimeMillis();
        }
        return head;
    }

    @Override
    public String getJournal(
            String fromRevisionId, String toRevisionId, String path)
            throws MicroKernelException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized String diff(
            String fromRevisionId, String toRevisionId, String path, int depth)
            throws MicroKernelException {
        NodeState before = getRoot(fromRevisionId);
        NodeState after = getRoot(toRevisionId);

        JsopDiff diff = new JsopDiff(null);
        after.compareAgainstBaseState(before, diff);
        return diff.toString();
    }

    @Override
    public boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
        return getNode(revisionId, path).exists();
    }

    @Override
    public long getChildNodeCount(String path, String revisionId)
            throws MicroKernelException {
        NodeState node = getNode(revisionId, path);
        if (node.exists()) {
            return node.getChildNodeCount(Long.MAX_VALUE);
        } else {
            throw new MicroKernelException(
                    "Node not found: " + revisionId + path);
        }
    }

    @Override
    public String getNodes(
            String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        NodeState node = getRoot(revisionId);
        for (String element : PathUtils.elements(path)) {
            node = node.getChildNode(element);
        }

        if (node.exists()) {
            JsopBuilder json = new JsopBuilder();
            if (maxChildNodes < 0) {
                maxChildNodes = Integer.MAX_VALUE;
            }
            serialize(getNode(revisionId, path), json, depth, offset, maxChildNodes);
            return json.toString();
        } else {
            return null;
        }
    }

    private void serialize(
            NodeState state, JsopBuilder json,
            int depth, long offset, int maxChildNodes) {
        json.object();

        for (PropertyState property : state.getProperties()) {
            json.key(property.getName());
            Type<?> type = property.getType();
            if (type.isArray()) {
                json.array();
                // FIXME
                json.endArray();
            } else if (type == Type.BOOLEAN) {
                json.value(property.getValue(Type.BOOLEAN).booleanValue());
            } else if (type == Type.LONG) {
                json.value(property.getValue(Type.LONG).longValue());
            } else if (type == Type.DOUBLE) {
                json.encodedValue(property.getValue(Type.DOUBLE).toString());
            } else {
                json.value(property.getValue(Type.STRING));
            }
        }

        json.key(":childNodeCount");
        json.value(state.getChildNodeCount(Long.MAX_VALUE));

        long index = 0;
        int count = 0;
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            if (index++ >= offset) {
                if (count++ >= maxChildNodes) {
                    break;
                }

                json.key(entry.getName());
                if (depth > 0) {
                    serialize(
                            entry.getNodeState(), json,
                            depth - 1, 0, maxChildNodes);
                } else {
                    json.object();
                    json.endObject();
                }
            }
        }

        json.endObject();
    }

    @Override
    public synchronized String commit(
            String path, String jsonDiff, String revisionId, String message)
            throws MicroKernelException {
        if (revisionId == null) {
            revisionId = head;
        }

        NodeState root = revisions.get(revisionId);
        if (root != null) {
            try {
                NodeBuilder builder = root.builder();
                applyJsop(builder, path, jsonDiff);
                NodeState newRoot = store.merge(
                        builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                NodeState oldRoot = revisions.get(head);
                if (!newRoot.equals(oldRoot)) {
                    String uuid = UUID.randomUUID().toString();
                    revisions.put(uuid, newRoot);
                    head = uuid;
                }

                return head;
            } catch (CommitFailedException e) {
                throw new MicroKernelException(e);
            }
        }

        NodeBuilder builder = branches.remove(revisionId);
        if (builder != null) {
            applyJsop(builder, path, jsonDiff);
            String uuid = UUID.randomUUID().toString();
            branches.put(uuid, builder);
            return uuid;
        }

        throw new MicroKernelException("Revision not found: " + revisionId);
    }

    @Override
    public synchronized String branch(String trunkRevisionId)
            throws MicroKernelException {
        String uuid = UUID.randomUUID().toString();
        branches.put(uuid, getRoot(trunkRevisionId).builder());
        return uuid;
    }

    @Override
    public synchronized String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        NodeBuilder builder = branches.remove(branchRevisionId);
        if (builder != null) {
            try {
                NodeState newRoot = store.merge(
                        builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                NodeState oldRoot = revisions.get(head);
                if (!newRoot.equals(oldRoot)) {
                    String uuid = UUID.randomUUID().toString();
                    revisions.put(uuid, newRoot);
                    head = uuid;
                }

                return head;
            } catch (CommitFailedException e) {
                throw new MicroKernelException(e);
            }
        } else {
            throw new MicroKernelException(
                    "Branch not found: " + branchRevisionId);
        }
    }

    @Override
    public String rebase(String branchRevisionId, String newBaseRevisionId)
            throws MicroKernelException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLength(String blobId) throws MicroKernelException {
        Blob blob = blobs.get(blobId);
        if (blob != null) {
            return blob.length();
        } else {
            throw new MicroKernelException("Blob not found: " + blobId);
        }
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length)
            throws MicroKernelException {
        Blob blob = blobs.get(blobId);
        if (blob != null) {
            try {
                InputStream stream = blob.getNewStream();
                try {
                    ByteStreams.skipFully(stream, pos);
                    return stream.read(buff, off, length);
                } finally {
                    stream.close();
                }
            } catch (IOException e) {
                throw new MicroKernelException("Failed to read a blob", e);
            }
        } else {
            throw new MicroKernelException("Blob not found: " + blobId);
        }
    }

    @Override
    public String write(InputStream in) throws MicroKernelException {
        try {
            String uuid = UUID.randomUUID().toString();
            blobs.put(uuid, store.createBlob(in));
            return uuid;
        } catch (IOException e) {
            throw new MicroKernelException("Failed to create a blob", e);
        }
    }

}
