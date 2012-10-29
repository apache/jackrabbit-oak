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
package org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper;

import java.io.InputStream;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.MicroKernelFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeMap;

/**
 * A microkernel prototype implementation that distributes nodes based on the path,
 * using a fixed configuration.
 * All mounted repositories contains the configuration as follows:
 * /:mount/rep1 { url: "mk:...", paths: "/a,/b" }.
 */
public class VirtualRepositoryWrapper extends MicroKernelWrapperBase implements MicroKernel {

    private static final String MOUNT = "/:mount";

    /**
     * The 'main' (wrapped) implementation.
     */
    private final MicroKernelWrapper mk;

    /**
     * Path map.
     * Key: path, value: mount name
     */
    private final TreeMap<String, String> dir = new TreeMap<String, String>();

    /**
     * Pending commit map.
     * Key: mount name, value: the builder that contains the pending commit.
     */
    private final HashMap<String, JsopWriter> builders = new HashMap<String, JsopWriter>();

    /**
     * Mount map.
     * Key: mount name, value: microkernel implementation.
     */
    private final HashMap<String, MicroKernelWrapper> mounts = new HashMap<String, MicroKernelWrapper>();

    /**
     * Head revision map.
     * Key: mount name, value: the head revision for this mount.
     */
    private final TreeMap<String, String> revisions = new TreeMap<String, String>();

    private final NodeMap map = new NodeMap();

    public VirtualRepositoryWrapper(MicroKernel mk) {
        this.mk = MicroKernelWrapperBase.wrap(mk);

        String head = mk.getHeadRevision();
        if (mk.nodeExists(MOUNT, head)) {
            String mounts = mk.getNodes(MOUNT, head, 1, 0, -1, null);
            NodeMap map = new NodeMap();
            JsopReader t = new JsopTokenizer(mounts);
            t.read('{');
            NodeImpl n = NodeImpl.parse(map, t, 0);
            for (long pos = 0;; pos++) {
                String childName = n.getChildNodeName(pos);
                if (childName == null) {
                    break;
                }
                NodeImpl mount = n.getNode(childName);
                String mountUrl = JsopTokenizer.decodeQuoted(mount.getProperty("url"));
                String[] paths = JsopTokenizer.decodeQuoted(mount.getProperty("paths")).split(",");
                addMount(childName, mountUrl, paths);
                getHeadRevision();
            }
        }
    }

    private void addMount(String mount, String url, String[] paths) {
        MicroKernel mk = MicroKernelFactory.getInstance(url);
        mounts.put(mount, MicroKernelWrapperBase.wrap(mk));
        for (String p : paths) {
            dir.put(p, mount);
        }
    }

    @Override
    public String commitStream(String rootPath, JsopReader t, String revisionId, String message) {
        while (true) {
            int r = t.read();
            if (r == JsopReader.END) {
                break;
            }
            String path = PathUtils.relativize("/", PathUtils.concat(rootPath, t.readString()));
            switch (r) {
            case '+':
                t.read(':');
                if (t.matches('{')) {
                    NodeImpl n = NodeImpl.parse(map, t, 0);
                    JsopWriter diff = new JsopBuilder();
                    diff.tag('+').key(path);
                    n.append(diff, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, false);
                    buffer(path, diff);
                } else {
                    String value = t.readRawValue().trim();
                    JsopWriter diff = new JsopBuilder();
                    diff.tag('+').key(path);
                    diff.encodedValue(value);
                    buffer(path, diff);
                }
                break;
            case '-': {
                JsopWriter diff = new JsopBuilder();
                diff.tag('-').value(path);
                buffer(path, diff);
                break;
            }
            case '^':
                t.read(':');
                String value;
                if (t.matches(JsopReader.NULL)) {
                    JsopWriter diff = new JsopBuilder();
                    diff.tag('^').key(path).value(null);
                    buffer(path, diff);
                } else {
                    value = t.readRawValue().trim();
                    JsopWriter diff = new JsopBuilder();
                    diff.tag('^').key(path).encodedValue(value);
                    buffer(path, diff);
                }
                break;
            case '>': {
                t.read(':');
                JsopWriter diff = new JsopBuilder();
                if (t.matches('{')) {
                    String position = t.readString();
                    t.read(':');
                    String to = t.readString();
                    t.read('}');
                    if (!PathUtils.isAbsolute(to)) {
                        to = PathUtils.concat(rootPath, to);
                    }
                    diff.tag('>').key(path);
                    diff.object().key(position);
                    diff.value(to).endObject();
                } else {
                    String to = t.readString();
                    if (!PathUtils.isAbsolute(to)) {
                        to = PathUtils.concat(rootPath, to);
                    }
                    diff.tag('>').key(path);
                    diff.value(to);
                }
                buffer(path, diff);
                break;
            }
            default:
                throw ExceptionFactory.get("token: " + (char) t.getTokenType());
            }
        }
        String revision = null;
        for (Entry<String, JsopWriter> e : builders.entrySet()) {
            String mount = e.getKey();
            MicroKernel m = mounts.get(mount);
            String jsop = e.getValue().toString();
            revision = m.commit("/", jsop, revisionId, message);
            revisions.put(mount, revision);
        }
        builders.clear();
        return getHeadRevision();
    }

    private String getMount(String path) {
        while (true) {
            String mount = dir.get(path);
            if (mount != null) {
                return mount;
            }
            // check parent
            if (PathUtils.denotesRoot(path)) {
                break;
            }
            path = PathUtils.getParentPath(path);
        }
        return null;
    }

    private void buffer(String path, JsopWriter diff) {
        String mount = getMount("/" + path);
        if (mount == null) {
            for (String m : mounts.keySet()) {
                getBuilder(m).append(diff).newline();
            }
        } else {
            getBuilder(mount).append(diff).newline();
        }
    }

    private JsopWriter getBuilder(String mount) {
        JsopWriter builder = builders.get(mount);
        if (builder == null) {
            builder = new JsopBuilder();
            builders.put(mount, builder);
        }
        return builder;
    }

    public void dispose() {
        for (MicroKernelWrapper wrapper : mounts.values()) {
            MicroKernelFactory.disposeInstance(MicroKernelWrapperBase.unwrap(wrapper));
        }
    }

    @Override
    public String getHeadRevision() {
        StringBuilder buff = new StringBuilder();
        if (revisions.size() != mounts.size()) {
            revisions.clear();
            for (Entry<String, MicroKernelWrapper> e : mounts.entrySet()) {
                String m = e.getKey();
                String r = e.getValue().getHeadRevision();
                revisions.put(m, r);
            }
        }
        for (Entry<String, String> e : revisions.entrySet()) {
            if (buff.length() > 0) {
                buff.append(',');
            }
            buff.append(e.getKey());
            buff.append(':');
            buff.append(e.getValue());
        }
        return buff.toString();
    }

    @Override
    public JsopReader getJournalStream(String fromRevisionId, String toRevisionId, String path) {
        return mk.getJournalStream(fromRevisionId, toRevisionId, path);
    }

    @Override
    public JsopReader diffStream(String fromRevisionId, String toRevisionId, String path, int depth) throws MicroKernelException {
        return mk.diffStream(fromRevisionId, toRevisionId, path, depth);
    }

    @Override
    public long getLength(String blobId) {
        return mk.getLength(blobId);
    }

    @Override
    public JsopReader getNodesStream(String path, String revisionId, int depth, long offset, int count, String filter) {
        String mount = getMount(path);
        if (mount == null) {
            // not mapped
            return null;
        }
        String rev = getRevision(mount, revisionId);
        MicroKernelWrapper mk = mounts.get(mount);
        return mk.getNodesStream(path, rev, depth, offset, count, filter);
    }

    private static String getRevision(String mount, String revisionId) {
        for (String rev : revisionId.split(",")) {
            if (rev.startsWith(mount + ":")) {
                return rev.substring(mount.length() + 1);
            }
        }
        throw ExceptionFactory.get("Unknown revision: " + revisionId + " mount: " + mount);
    }

    @Override
    public JsopReader getRevisionsStream(long since, int maxEntries, String path) {
        return mk.getRevisionsStream(since, maxEntries, path);
    }

    @Override
    public boolean nodeExists(String path, String revisionId) {
        String mount = getMount(path);
        if (mount == null) {
            throw ExceptionFactory.get("Not mapped: " + path);
        }
        String rev = getRevision(mount, revisionId);
        MicroKernel mk = mounts.get(mount);
        return mk.nodeExists(path, rev);
    }

    @Override
    public long getChildNodeCount(String path, String revisionId) {
        String mount = getMount(path);
        if (mount == null) {
            throw ExceptionFactory.get("Not mapped: " + path);
        }
        String rev = getRevision(mount, revisionId);
        MicroKernel mk = mounts.get(mount);
        return mk.getChildNodeCount(path, rev);
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length) {
        return mk.read(blobId, pos, buff, off, length);
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long maxWaitMillis) throws InterruptedException {
        return mk.waitForCommit(oldHeadRevisionId, maxWaitMillis);
    }

    @Override
    public String write(InputStream in) {
        return mk.write(in);
    }

    @Override
    public String branch(String trunkRevisionId) {
        // TODO OAK-45 support
        return mk.branch(trunkRevisionId);
    }

    @Override
    public String merge(String branchRevisionId, String message) {
        // TODO OAK-45 support
        return mk.merge(branchRevisionId, message);
    }
}

