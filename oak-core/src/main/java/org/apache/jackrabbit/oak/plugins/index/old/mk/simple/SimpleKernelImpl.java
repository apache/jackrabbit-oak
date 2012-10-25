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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.AbstractBlobStore;
import org.apache.jackrabbit.mk.blobs.FileBlobStore;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mk.server.Server;
import org.apache.jackrabbit.mk.util.Cache;
import org.apache.jackrabbit.mk.util.CommitGate;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper.MicroKernelWrapperBase;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;

/*

Node structure:

/head/rev = 100
/head/nanos = nanos
/head/commit/diff = [+ "/test"{}]
/head/commit/msg = "hello ..."
/head/config/ (optional)
/head/data/
/99/head/nanos = nanos
/99/98/head
/99/98/97/head
/99/90/head
/99/90/89/head

*/

/**
 * A simple MicroKernel implementation.
 */
public class SimpleKernelImpl extends MicroKernelWrapperBase implements MicroKernel {

    private static final int REV_SKIP_OFFSET = 20;

    private final String name;
    private final AbstractBlobStore ds;
    private final AscendingClock clock = new AscendingClock(System.currentTimeMillis());
    private final CommitGate gate = new CommitGate();
    private final Cache<Long, Revision> revisionCache = Cache.newInstance(null, 1024 * 1024);

    private volatile long headRevId;
    private volatile String headRevision;
    private NodeMap nodeMap;
    private Server server;
    private boolean disposed;

    public SimpleKernelImpl(String name) {
        this.name = name;
        boolean startServer = false;
        if (name.startsWith("server:")) {
            startServer = true;
            name = name.substring("server:".length());
        }
        nodeMap = new NodeMap();
        if (name.startsWith("fs:")) {
            String dir = name.substring("fs:".length());
            try {
                ds = new FileBlobStore(dir);
            } catch (IOException e) {
                throw ExceptionFactory.convert(e);
            }
            nodeMap = new NodeMapInDb(dir);
        } else {
            ds = new MemoryBlobStore();
        }
        if (nodeMap.getRootId() == null) {
            NodeImpl head = new NodeImpl(nodeMap, 0);
            Revision revNode = new Revision(0, 0, "", "");
            head = revNode.store(head, new NodeImpl(nodeMap, 0));
            head.addChildNode("data", new NodeImpl(nodeMap, 0));
            NodeImpl root = new NodeImpl(nodeMap, 0);
            root.addChildNode("head", head);
            nodeMap.commit(root);
        } else {
            NodeImpl head = getRoot().getNode("head");
            String rev = head.getProperty("rev");
            headRevId = Revision.parseId(JsopTokenizer.decodeQuoted(rev));
            applyConfig(head);
        }
        headRevision = Revision.formatId(headRevId);
        if (startServer) {
            server = new Server(this);
            try {
                server.start();
            } catch (IOException e) {
                throw ExceptionFactory.convert(e);
            }
        }
    }

    private void applyConfig(NodeImpl head) {
        // /head/config doesn't always exist
        if (head.exists("config")) {
            NodeImpl config = head.getNode("config");
            for (int i = 0, size = config.getPropertyCount(); i < size; i++) {
                nodeMap.setSetting(config.getProperty(i), config.getPropertyValue(i));
            }
        }
    }

    @Override
    public synchronized String commitStream(String rootPath, JsopReader jsonDiff, String revisionId, String message) {
        revisionId = revisionId == null ? headRevision : revisionId;

        // TODO message should be json
        // TODO read / write version
        // TODO getJournal and getRevision don't have a path,
        // which means we can't implement access rights using path prefixes
        try {
            return doCommit(rootPath, jsonDiff, revisionId, message);
        } catch (Exception e) {
            throw ExceptionFactory.convert(e);
        }
    }

    private String doCommit(String rootPath, JsopReader t, String revisionId, String message) {
        long oldRevision = headRevId, rev = headRevId + 1;
        NodeImpl root = nodeMap.getRootId().getNode(nodeMap);
        NodeImpl head = root.getNode("head"), oldHead = head;
        NodeImpl data = head.getNode("data");
        JsopWriter diff = new JsopStream();
        while (true) {
            int r = t.read();
            if (r == JsopReader.END) {
                break;
            }
            String path = PathUtils.concat(rootPath, t.readString());
            String from = PathUtils.relativize("/", path);
            switch (r) {
            case '+':
                t.read(':');
                diff.tag('+').key(path);
                if (t.matches('{')) {
                    NodeImpl n = NodeImpl.parse(nodeMap, t, rev);
                    data = data.cloneAndAddChildNode(from, false, null, n, rev);
                    n.append(diff, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, false);
                } else {
                    String value = t.readRawValue().trim();
                    String nodeName = PathUtils.getParentPath(from);
                    String propertyName = PathUtils.getName(from);
                    if (data.getNode(nodeName).hasProperty(propertyName)) {
                        throw ExceptionFactory.get("Property already exists: " + propertyName);
                    }
                    data = data.cloneAndSetProperty(from, value, rev);
                    diff.encodedValue(value);
                }
                diff.newline();
                break;
            case '-':
                diff.tag('-').value(path).newline();
                if (data.exists(from) || !getRevisionDataRoot(revisionId).exists(from)) {
                    // this will fail if the node didn't exist
                    data = data.cloneAndRemoveChildNode(from, rev);
                }
                break;
            case '^':
                t.read(':');
                boolean isConfigChange = from.startsWith(":root/head/config/");
                String value;
                if (t.matches(JsopReader.NULL)) {
                    value = null;
                    diff.tag('^').key(path).value(null);
                } else {
                    value = t.readRawValue().trim();
                    String nodeName = PathUtils.getParentPath(from);
                    String propertyName = PathUtils.getName(from);
                    if (isConfigChange || data.getNode(nodeName).hasProperty(propertyName)) {
                        diff.tag('^');
                    } else {
                        diff.tag('+');
                    }
                    diff.key(path).encodedValue(value);
                }
                if (isConfigChange) {
                    String p = PathUtils.relativize(":root/head", from);
                    if (!head.exists("config")) {
                        head = head.setChild("config", new NodeImpl(nodeMap, rev), rev);
                    }
                    head = head.cloneAndSetProperty(p, value, rev);
                    applyConfig(head);
                } else {
                    data = data.cloneAndSetProperty(from, value, rev);
                }
                diff.newline();
                break;
            case '>': {
                t.read(':');
                diff.tag('>').key(path);
                String name = PathUtils.getName(from);
                String position, target, to;
                boolean rename;
                if (t.matches('{')) {
                    rename = false;
                    position = t.readString();
                    t.read(':');
                    target = t.readString();
                    t.read('}');
                    diff.object().key(position);
                    if (!PathUtils.isAbsolute(target)) {
                        target = PathUtils.concat(rootPath, target);
                    }
                    diff.value(target).endObject();
                } else {
                    rename = true;
                    position = null;
                    target = t.readString();
                    if (!PathUtils.isAbsolute(target)) {
                        target = PathUtils.concat(rootPath, target);
                    }
                    diff.value(target);
                }
                diff.newline();
                boolean before = false;
                if ("last".equals(position)) {
                    target = PathUtils.concat(target, name);
                    position = null;
                } else if ("first".equals(position)) {
                    target = PathUtils.concat(target, name);
                    position = null;
                    before = true;
                } else if ("before".equals(position)) {
                    position = PathUtils.getName(target);
                    target = PathUtils.getParentPath(target);
                    target = PathUtils.concat(target, name);
                    before = true;
                } else if ("after".equals(position)) {
                    position = PathUtils.getName(target);
                    target = PathUtils.getParentPath(target);
                    target = PathUtils.concat(target, name);
                } else if (position == null) {
                    // move
                } else {
                    throw ExceptionFactory.get("position: " + position);
                }
                to = PathUtils.relativize("/", target);
                boolean inPlaceRename = false;
                if (rename) {
                    if (PathUtils.getParentPath(from).equals(PathUtils.getParentPath(to))) {
                        inPlaceRename = true;
                        position = PathUtils.getName(from);
                    }
                }
                NodeImpl node = data.getNode(from);
                if (!inPlaceRename) {
                    data = data.cloneAndRemoveChildNode(from, rev);
                }
                data = data.cloneAndAddChildNode(to, before, position, node, rev);
                if (inPlaceRename) {
                    data = data.cloneAndRemoveChildNode(from, rev);
                }
                break;
            }
            case '*': {
                // TODO possibly support target position notation
                t.read(':');
                String target = t.readString();
                if (!PathUtils.isAbsolute(target)) {
                    target = PathUtils.concat(rootPath, target);
                }
                diff.tag('*').key(path).value(target);
                String to = PathUtils.relativize("/", target);
                NodeImpl node = data.getNode(from);
                JsopStream json = new JsopStream();
                node.append(json, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, false);
                json.read('{');
                NodeImpl n2 = NodeImpl.parse(nodeMap, json, rev);
                data = data.cloneAndAddChildNode(to, false, null, n2, rev);
                break;
            }
            default:
                throw ExceptionFactory.get("token: " + (char) t.getTokenType());
            }
        }
        head = head.setChild("data", data, rev);
        Revision revNode = new Revision(rev, clock.nanoTime(), diff.toString(), message);
        revisionCache.put(rev, revNode);
        head = revNode.store(head, new NodeImpl(nodeMap, rev));
        root = root.setChild("head", head, rev);
        String old = Revision.formatId(oldRevision);
        NodeImpl oldRev = new NodeImpl(nodeMap, rev);
        oldRev.addChildNode("head", oldHead);
        String lastRev = Revision.formatId(oldRevision - 1);
        if (root.exists(lastRev)) {
            NodeImpl lastRevNode = root.getNode(lastRev);
            root = root.cloneAndRemoveChildNode(lastRev, rev);
            oldRev.setChild(lastRev, lastRevNode, rev);
            if (oldRevision % REV_SKIP_OFFSET == 0) {
                long skip = oldRevision - REV_SKIP_OFFSET;
                NodeImpl n = getRevisionNode(getRoot(), skip, skip);
                if (n != null) {
                    oldRev.setChild(Revision.formatId(skip), n, rev);
                    // TODO remove old link to reduce descendant count
                }
            }
        }
        root = root.setChild(old, oldRev, rev);
        nodeMap.commit(root);
        headRevId = rev;
        headRevision = Revision.formatId(rev);
        gate.commit(headRevision);
        return headRevision;
    }

    private NodeImpl getRoot() {
        return nodeMap.getRootId().getNode(nodeMap);
    }

    @Override
    public String getHeadRevision() {
        return headRevision;
    }

    @Override
    public JsopReader getRevisionsStream(long since, int maxEntries, String path) {
        NodeImpl node = getRoot();
        long sinceNanos = since * 1000000;
        ArrayList<Revision> revisions = new ArrayList<Revision>();
        Revision r = Revision.get(node.getNode("head"));
        if (sinceNanos < r.getNanos() && maxEntries > 0) {
            revisions.add(r);
            while (revisions.size() < maxEntries) {
                String newest = null;
                for (int i = 0;; i++) {
                    String next = node.getChildNodeName(i);
                    if (next == null) {
                        break;
                    } else if (!next.equals("head") && !next.equals("config")) {
                        if (newest == null || next.compareTo(newest) >= 0) {
                            newest = next;
                        }
                    }
                }
                if (newest != null) {
                    r = Revision.get(node);
                    if (r == null) {
                        break;
                    }
                    revisions.add(r);
                }
            }
        }
        JsopStream buff = new JsopStream().array();
        Collections.sort(revisions);
        for (Revision rev : revisions) {
            if (rev.getNanos() > sinceNanos) {
                buff.encodedValue(rev.toString());
            }
        }
        return buff.endArray();
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long maxWaitMillis) throws InterruptedException {
        return gate.waitForCommit(oldHeadRevisionId, maxWaitMillis);
    }

    @Override
    public JsopReader getJournalStream(String fromRevisionId, String toRevisionId, String path) {
        fromRevisionId = fromRevisionId == null ? headRevision : fromRevisionId;
        toRevisionId = toRevisionId == null ? headRevision : toRevisionId;

        long fromRev = Revision.parseId(fromRevisionId);
        long toRev = Revision.parseId(toRevisionId);
        NodeImpl node = getRoot();
        ArrayList<Revision> revisions = new ArrayList<Revision>();
        Revision r = Revision.get(node.getNode("head"));
        if (r.getId() >= fromRev && r.getId() <= toRev) {
            revisions.add(r);
        }
        if (r.getId() > fromRev) {
            node = getRevisionNode(node, fromRev, toRev);
            while (node != null) {
                r = Revision.get(node.getNode("head"));
                if (r.getId() >= fromRev && r.getId() <= toRev) {
                    r = revisionCache.replace(r.getId(), r);
                    revisions.add(r);
                }
                String next = Revision.formatId(r.getId() - 1);
                if (!node.exists(next)) {
                    break;
                }
                node = node.getNode(next);
            }
        }
        JsopStream buff = new JsopStream().array().newline();
        for (int i = revisions.size() - 1; i >= 0; i--) {
            Revision rev = revisions.get(i);
            if (rev.getId() >= fromRev && rev.getId() <= toRev) {
                rev.appendJournal(buff);
            }
        }
        return buff.endArray();
   }

    private static NodeImpl getRevisionNode(NodeImpl node, long fromRev, long toRev) {
        while (true) {
            long next = -1;
            String nextRev = null;
            for (int i = 0;; i++) {
                String n = node.getChildNodeName(i);
                if (n == null) {
                    break;
                }
                if ("head".equals(n)) {
                    continue;
                }
                long rev = Revision.parseId(n);
                if (next == -1 || (rev >= toRev && (rev < next || next < toRev))) {
                    next = rev;
                    nextRev = n;
                }
            }
            if (next == -1 || fromRev > next) {
                return null;
            } else {
                node = node.getNode(nextRev);
                if (next <= toRev) {
                    return node;
                }
            }
        }
    }


    @Override
    public JsopReader diffStream(String fromRevisionId, String toRevisionId, String path, int depth) {
        fromRevisionId = fromRevisionId == null ? headRevision : fromRevisionId;
        toRevisionId = toRevisionId == null ? headRevision : toRevisionId;
        // TODO implement if required
        return new JsopStream();
    }

    /**
     * Get the nodes. The following prefixes are supported:
     * <ul><li>:root - get the root node (including all old revisions)
     * </li><li>:info - get internal info such as the node count
     * </li></ul>
     *
     * @param path the path
     * @param revisionId the revision
     * @return the json string
     */
    @Override
    public JsopReader getNodesStream(String path, String revisionId, int depth, long offset, int count, String filter) {
        revisionId = revisionId == null ? headRevision : revisionId;

        // TODO offset > 0 should mean the properties are not included
        if (count < 0) {
            count = nodeMap.getMaxMemoryChildren();
        }
        if (!PathUtils.isAbsolute(path)) {
            throw ExceptionFactory.get("Not an absolute path: " + path);
        }
        NodeImpl n;
        if (path.startsWith("/:")) {
            if (path.startsWith("/:root")) {
                n = getRoot().getNode(PathUtils.relativize("/:root", path));
            } else if (path.startsWith("/:info")) {
                n = nodeMap.getInfo(PathUtils.relativize("/:info", path));
            } else {
                n = getRevisionDataRoot(revisionId).getNode(path.substring(1));
            }
        } else {
            n = getRevisionDataRoot(revisionId).getNode(path.substring(1));
        }
        if (n == null) {
            return null;
        }
        JsopStream json = new JsopStream();
        n.append(json, depth, offset, count, true);
        return json;
    }

    private NodeImpl getRevisionDataRoot(String revisionId) {
        NodeImpl rev = getRevisionIfExists(revisionId);
        if (rev == null) {
            throw ExceptionFactory.get("Revision not found: " + revisionId);
        }
        rev = rev.getNode("data");
        return rev;
    }

    private NodeImpl getRevisionIfExists(String revisionId) {
        NodeImpl node = getRoot();
        NodeImpl head = node.getNode("head");
        String headRev;
        headRev = head.getProperty("rev");
        headRev = headRev == null ? null : JsopTokenizer.decodeQuoted(headRev);
        // we can't rely on headRevId, as it's a volatile field
        if (revisionId.equals(headRev)) {
            return head;
        } else {
            long rev = Revision.parseId(revisionId);
            NodeImpl rnode = getRevisionNode(node, rev, rev);
            if (rnode != null) {
                return rnode.getNode("head");
            } else {
                return null;
            }
        }
    }

    @Override
    public boolean nodeExists(String path, String revisionId) {
        revisionId = revisionId == null ? headRevision : revisionId;

        if (!PathUtils.isAbsolute(path)) {
            throw ExceptionFactory.get("Not an absolute path: " + path);
        }
        if (PathUtils.denotesRoot(path)) {
            return true;
        } else if (path.equals("/:info")) {
            return true;
        }
        return getRevisionDataRoot(revisionId).exists(path.substring(1));
    }

    @Override
    public long getChildNodeCount(String path, String revisionId) {
        revisionId = revisionId == null ? headRevision : revisionId;

        if (!PathUtils.isAbsolute(path)) {
            throw ExceptionFactory.get("Not an absolute path: " + path);
        }
        return getRevisionDataRoot(revisionId).getNode(path).getChildNodeCount();
    }

    @Override
    public long getLength(String blobId) {
        try {
            return ds.getBlobLength(blobId);
        } catch (Exception e) {
            throw ExceptionFactory.convert(e);
        }
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length) {
        try {
            return ds.readBlob(blobId, pos, buff, off, length);
        } catch (Exception e) {
            throw ExceptionFactory.convert(e);
        }
    }

    @Override
    public String write(InputStream in) {
        try {
            return ds.writeBlob(in);
        } catch (Exception e) {
            throw ExceptionFactory.convert(e);
        }
    }

    public synchronized void dispose() {
        if (!disposed) {
            disposed = true;
            gate.commit("end");
            nodeMap.close();
            if (server != null) {
                server.stop();
                server = null;
            }
        }
    }

    @Override
    public String toString() {
        return "simple:" + name;
    }

    @Override
    public String branch(String trunkRevisionId) throws MicroKernelException {
        trunkRevisionId = trunkRevisionId == null ? headRevision : trunkRevisionId;

        // TODO OAK-45 support
        throw new UnsupportedOperationException();
    }

    @Override
    public String merge(String branchRevisionId, String message) throws MicroKernelException {
        // TODO OAK-45 support
        throw new UnsupportedOperationException();
    }
}
