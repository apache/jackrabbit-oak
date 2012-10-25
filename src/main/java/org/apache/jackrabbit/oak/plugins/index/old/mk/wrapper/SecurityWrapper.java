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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mk.util.SimpleLRUCache;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeMap;

import java.io.InputStream;

/**
 * A microkernel prototype implementation that filters nodes based on simple
 * access rights. Each user has a password, and (optionally) a list of rights,
 * stored as follows: /:user/x { password: "123", rights: "a" } Each node can
 * require the user has a certain right: /data { ":right": "a" } Access rights
 * are recursive. There is a special right "admin" which means everything is
 * allowed, and "write" meaning a user can write.
 * <p>
 * This implementation is not meant for production, it is only used to find
 * (performance and other) problems when using such an approach.
 */
public class SecurityWrapper extends MicroKernelWrapperBase implements MicroKernel {

    private final MicroKernelWrapper mk;
    private final boolean admin, write;
    private final String[] userRights;
    private final NodeMap map = new NodeMap();
    private final SimpleLRUCache<String, NodeImpl> cache = SimpleLRUCache.newInstance(100);
    private String rightsRevision;

    /**
     * Decorates the given {@link MicroKernel} with authentication and
     * authorization. The responsibility of properly disposing the given
     * MicroKernel instance remains with the caller.
     *
     * @param mk the wrapped kernel
     * @param user the user name
     * @param pass the password
     */
    public SecurityWrapper(MicroKernel mk, String user, String pass) {
        this.mk = MicroKernelWrapperBase.wrap(mk);
        // TODO security for the index mechanism

        String role = mk.getNodes("/:user/" + user, mk.getHeadRevision(), 1, 0, -1, null);
        NodeMap map = new NodeMap();
        JsopReader t = new JsopTokenizer(role);
        t.read('{');
        NodeImpl n = NodeImpl.parse(map, t, 0);
        String password = JsopTokenizer.decodeQuoted(n.getProperty("password"));
        if (!pass.equals(password)) {
            throw ExceptionFactory.get("Wrong password");
        }
        String[] rights =
                JsopTokenizer.decodeQuoted(n.getProperty("rights")).split(",");
        this.userRights = rights;
        boolean isAdmin = false;
        boolean canWrite = false;
        for (String r : rights) {
            if (r.equals("admin")) {
                isAdmin = true;
            } else if (r.equals("write")) {
                canWrite = true;
            }
        }
        this.admin = isAdmin;
        this.write = canWrite;
    }

    @Override
    public String commitStream(String rootPath, JsopReader jsonDiff, String revisionId, String message) {
        checkRights(rootPath, true);
        if (!admin) {
            verifyDiff(jsonDiff, revisionId, rootPath, null);
        }
        return mk.commitStream(rootPath, jsonDiff, revisionId, message);
    }

    public void dispose() {
        // do nothing
    }

    @Override
    public String getHeadRevision() {
        return mk.getHeadRevision();
    }

    @Override
    public JsopReader getJournalStream(String fromRevisionId, String toRevisionId, String path) {
        rightsRevision = getHeadRevision();
        JsopReader t = mk.getJournalStream(fromRevisionId, toRevisionId, path);
        if (admin) {
            return t;
        }
        t.read('[');
        if (t.matches(']')) {
            return new JsopTokenizer("[]");
        }
        JsopStream buff = new JsopStream();
        buff.array();
        String revision = fromRevisionId;
        do {
            t.read('{');
            buff.object();
            do {
                String key = t.readString();
                buff.key(key);
                t.read(':');
                if (key.equals("id")) {
                    t.read();
                    String value = t.getToken();
                    revision = value;
                    buff.value(value);
                } else if (key.equals("changes")) {
                    t.read();
                    String value = t.getToken();
                    value = filterDiff(new JsopTokenizer(value), revision).toString();
                    buff.value(value);
                } else {
                    String raw = t.readRawValue();
                    //System.out.println(key + ":" + raw);
                    buff.encodedValue(raw);
                }
            } while (t.matches(','));
            buff.endObject();
            t.read('}');
        } while (t.matches(','));
        buff.endArray();
        return buff;
    }

    @Override
    public JsopReader diffStream(String fromRevisionId, String toRevisionId, String path, int depth) {
        rightsRevision = getHeadRevision();
        JsopReader diff = mk.diffStream(fromRevisionId, toRevisionId, path, depth);
        if (admin) {
            return diff;
        }
        return filterDiff(diff, toRevisionId);
    }

    private JsopReader filterDiff(JsopReader t, String revisionId) {
        JsopStream buff = new JsopStream();
        verifyDiff(t, revisionId, null, buff);
        return buff;
    }

    private void verifyDiff(JsopReader t, String revisionId, String rootPath, JsopWriter diff) {
        while (true) {
            int r = t.read();
            if (r == JsopReader.END) {
                break;
            }
            String path;
            if (rootPath == null) {
                path = t.readString();
            } else {
                path = PathUtils.concat(rootPath, t.readString());
            }
            switch (r) {
            case '+':
                t.read(':');
                if (t.matches('{')) {
                    NodeImpl n = NodeImpl.parse(map, t, 0);
                    if (checkDiff(path, diff)) {
                        diff.tag('+').key(path);
                        n = filterAccess(path, n);
                        n.append(diff, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, false);
                        diff.newline();
                    }
                } else {
                    String value = t.readRawValue().trim();
                    String nodeName = PathUtils.getParentPath(path);
                    if (checkDiff(nodeName, diff)) {
                        if (checkPropertyRights(path)) {
                            diff.tag('+').key(path);
                            diff.encodedValue(value);
                            diff.newline();
                        }
                    }
                }
                break;
            case '-':
                if (checkDiff(path, diff)) {
                    diff.tag('-').value(path);
                    diff.newline();
                }
                break;
            case '^':
                t.read(':');
                String value;
                if (t.matches(JsopReader.NULL)) {
                    if (checkDiff(path, diff)) {
                        if (checkPropertyRights(path)) {
                            diff.tag('^').key(path).value(null);
                            diff.newline();
                        }
                    }
                } else {
                    value = t.readRawValue().trim();
                    String nodeName = PathUtils.getParentPath(path);
                    if (checkDiff(nodeName, diff)) {
                        if (checkPropertyRights(path)) {
                            diff.tag('^').key(path).encodedValue(value);
                            diff.newline();
                        }
                    }
                }
                break;
            case '>':
                t.read(':');
                checkDiff(path, diff);
                String name = PathUtils.getName(path);
                if (t.matches('{')) {
                    String position = t.readString();
                    t.read(':');
                    String to = t.readString();
                    String target;
                    t.read('}');
                    if (!PathUtils.isAbsolute(to)) {
                        to = PathUtils.concat(rootPath, to);
                    }
                    if ("last".equals(position) || "first".equals(position)) {
                        target = PathUtils.concat(to, name);
                    } else {
                        // before, after
                        target = PathUtils.getParentPath(to);
                    }
                    if (checkDiff(target, diff)) {
                        diff.tag('>').key(path);
                        diff.object().key(position);
                        diff.value(to).endObject();
                        diff.newline();
                    }
                } else {
                    String to = t.readString();
                    if (!PathUtils.isAbsolute(to)) {
                        to = PathUtils.concat(rootPath, to);
                    }
                    if (checkDiff(to, diff)) {
                        diff.tag('>').key(path);
                        diff.value(to);
                        diff.newline();
                    }
                }
                break;
            default:
                throw ExceptionFactory.get("token: " + (char) t.getTokenType());
            }
        }
    }

    private boolean checkDiff(String path, JsopWriter target) {
        if (checkRights(path, target == null)) {
            return target != null;
        } else if (target == null) {
            throw ExceptionFactory.get("Access denied");
        }
        return false;
    }

    @Override
    public long getLength(String blobId) {
        return mk.getLength(blobId);
    }

    @Override
    public JsopReader getNodesStream(String path, String revisionId, int depth, long offset, int count, String filter) {
        rightsRevision = getHeadRevision();
        if (!checkRights(path, false)) {
            return null;
        }
        JsopReader t = mk.getNodesStream(path, revisionId, depth, offset, count, filter);
        if (admin || t == null) {
            return t;
        }
        t.read('{');
        NodeImpl n = NodeImpl.parse(map, t, 0);
        n = filterAccess(path, n);
        JsopStream buff = new JsopStream();
        if (n == null) {
            return null;
        } else {
            // TODO childNodeCount properties might be wrong
            // when count and offset are used
            n.append(buff, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, true);
        }
        return buff;
    }

    @Override
    public JsopReader getRevisionsStream(long since, int maxEntries, String path) {
        return mk.getRevisionsStream(since, maxEntries, path);
    }

    @Override
    public boolean nodeExists(String path, String revisionId) {
        rightsRevision = getHeadRevision();
        if (!checkRights(path, false)) {
            return false;
        }
        return mk.nodeExists(path, revisionId);
    }

    @Override
    public long getChildNodeCount(String path, String revisionId) {
        rightsRevision = getHeadRevision();
        if (!checkRights(path, false)) {
            throw ExceptionFactory.get("Node not found: " + path);
        }
        return mk.getChildNodeCount(path, revisionId);
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
        rightsRevision = getHeadRevision();
        checkRights(null, true);
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

    private NodeImpl filterAccess(String path, NodeImpl n) {
        if (!checkRights(path, false)) {
            return null;
        }
        if (!admin && n.hasProperty(":rights")) {
            n = n.cloneAndSetProperty(":rights", null, 0);
        }
        for (long pos = 0;; pos++) {
            String childName = n.getChildNodeName(pos);
            if (childName == null) {
                break;
            }
            NodeImpl c = n.getNode(childName);
            NodeImpl c2 = filterAccess(PathUtils.concat(path, childName), c);
            if (c2 != c) {
                if (c2 == null) {
                    n = n.cloneAndRemoveChildNode(childName, 0);
                } else {
                    n = n.setChild(childName, c2, 0);
                }
            }
        }
        return n;
    }

    private static boolean checkPropertyRights(String path) {
        return !PathUtils.getName(path).equals(":rights");
    }

    private boolean checkRights(String path, boolean write) {
        if (admin) {
            return true;
        }
        if (write && !this.write) {
            return false;
        }
        if (path == null) {
            return true;
        }
        boolean access = false;
        while (true) {
            String key = path + "@" + rightsRevision;
            NodeImpl n = cache.get(key);
            if (n == null) {
                if (mk.nodeExists(path, rightsRevision)) {
                    String json = mk.getNodes(path, rightsRevision, 0, 0, 0, null);
                    JsopReader t = new JsopTokenizer(json);
                    t.read('{');
                    n = NodeImpl.parse(map, t, 0);
                } else {
                    n = new NodeImpl(map, 0);
                }
                cache.put(key, n);
            }
            Boolean b = hasRights(n);
            if (b != null) {
                if (b) {
                    access = true;
                } else {
                    return false;
                }
            }
            // check parent
            if (PathUtils.denotesRoot(path)) {
                break;
            }
            path = PathUtils.getParentPath(path);
        }
        return access;
    }

    private Boolean hasRights(NodeImpl n) {
        String rights = n.getProperty(":rights");
        if (rights == null) {
            return null;
        }
        rights = JsopTokenizer.decodeQuoted(rights);
        for (String r : rights.split(",")) {
            boolean got = false;
            for (String u : userRights) {
                if (u.equals(r)) {
                    got = true;
                    break;
                }
            }
            if (!got) {
                return false;
            }
        }
        return true;
    }

}
