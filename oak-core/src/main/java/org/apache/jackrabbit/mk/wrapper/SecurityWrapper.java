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
package org.apache.jackrabbit.mk.wrapper;

import java.io.InputStream;
import org.apache.jackrabbit.mk.MicroKernelFactory;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.mk.simple.NodeMap;
import org.apache.jackrabbit.mk.util.ExceptionFactory;
import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.mk.util.SimpleLRUCache;

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
public class SecurityWrapper extends WrapperBase implements MicroKernel {

    private final Wrapper mk;
    private final boolean admin, write;
    private final String[] userRights;
    private final NodeMap map = new NodeMap();
    private final SimpleLRUCache<String, NodeImpl> cache = SimpleLRUCache.newInstance(100);
    private String rightsRevision;

    private SecurityWrapper(MicroKernel mk, String[] rights) {
        // TODO security for the index mechanism
        this.mk = WrapperBase.wrap(mk);
        this.userRights = rights;
        boolean isAdmin = false, canWrite = false;
        for (String r : rights) {
            if (r.equals("admin")) {
                isAdmin = true;
            } else if (r.equals("write")) {
                canWrite = true;
            }
        }
        admin = isAdmin;
        write = canWrite;
    }

    public static synchronized SecurityWrapper get(String url) {
        String userPassUrl = url.substring("sec:".length());
        int index = userPassUrl.indexOf(':');
        if (index < 0) {
            throw ExceptionFactory.get("Expected url format: sec:user@pass:<url>");
        }
        String u = userPassUrl.substring(index + 1);
        String userPass = userPassUrl.substring(0, index);
        index = userPass.indexOf('@');
        if (index < 0) {
            throw ExceptionFactory.get("Expected url format: sec:user@pass:<url>");
        }
        String user = userPass.substring(0, index);
        String pass = userPass.substring(index + 1);
        MicroKernel mk = MicroKernelFactory.getInstance(u);
        try {
            String role = mk.getNodes("/:user/" + user, mk.getHeadRevision());
            NodeMap map = new NodeMap();
            JsopReader t = new JsopTokenizer(role);
            t.read('{');
            NodeImpl n = NodeImpl.parse(map, t, 0);
            String password = JsopTokenizer.decodeQuoted(n.getProperty("password"));
            if (!pass.equals(password)) {
                throw ExceptionFactory.get("Wrong password");
            }
            String rights = JsopTokenizer.decodeQuoted(n.getProperty("rights"));
            return new SecurityWrapper(mk, rights.split(","));
        } catch (MicroKernelException e) {
            mk.dispose();
            throw e;
        }
    }

    public String commitStream(String rootPath, JsopReader jsonDiff, String revisionId, String message) {
        checkRights(rootPath, true);
        if (!admin) {
            verifyDiff(jsonDiff, revisionId, rootPath, null);
        }
        return mk.commitStream(rootPath, jsonDiff, revisionId, message);
    }

    public void dispose() {
        mk.dispose();
    }

    public String getHeadRevision() {
        return mk.getHeadRevision();
    }

    public JsopReader getJournalStream(String fromRevisionId, String toRevisionId, String filter) {
        rightsRevision = getHeadRevision();
        JsopReader t = mk.getJournalStream(fromRevisionId, toRevisionId, filter);
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

    public JsopReader diffStream(String fromRevisionId, String toRevisionId, String path) {
        rightsRevision = getHeadRevision();
        JsopReader diff = mk.diffStream(fromRevisionId, toRevisionId, path);
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
            if (r == JsopTokenizer.END) {
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
                if (t.matches(JsopTokenizer.NULL)) {
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

    public long getLength(String blobId) {
        return mk.getLength(blobId);
    }

    public JsopReader getNodesStream(String path, String revisionId) {
        return getNodesStream(path, revisionId, 1, 0, -1, null);
    }

    public JsopReader getNodesStream(String path, String revisionId, int depth, long offset, int count, String filter) {
        rightsRevision = getHeadRevision();
        if (!checkRights(path, false)) {
            throw ExceptionFactory.get("Node not found: " + path);
        }
        JsopReader t = mk.getNodesStream(path, revisionId, depth, offset, count, filter);
        if (admin) {
            return t;
        }
        t.read('{');
        NodeImpl n = NodeImpl.parse(map, t, 0);
        n = filterAccess(path, n);
        JsopStream buff = new JsopStream();
        if (n == null) {
            throw ExceptionFactory.get("Node not found: " + path);
        } else {
            // TODO childNodeCount properties might be wrong
            // when count and offset are used
            n.append(buff, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, true);
        }
        return buff;
    }

    public JsopReader getRevisionsStream(long since, int maxEntries) {
        return mk.getRevisionsStream(since, maxEntries);
    }

    public boolean nodeExists(String path, String revisionId) {
        rightsRevision = getHeadRevision();
        if (!checkRights(path, false)) {
            return false;
        }
        return mk.nodeExists(path, revisionId);
    }

    public long getChildNodeCount(String path, String revisionId) {
        rightsRevision = getHeadRevision();
        if (!checkRights(path, false)) {
            throw ExceptionFactory.get("Node not found: " + path);
        }
        return mk.getChildNodeCount(path, revisionId);
    }

    public int read(String blobId, long pos, byte[] buff, int off, int length) {
        return mk.read(blobId, pos, buff, off, length);
    }

    public String waitForCommit(String oldHeadRevision, long maxWaitMillis) throws InterruptedException {
        return mk.waitForCommit(oldHeadRevision, maxWaitMillis);
    }

    public String write(InputStream in) {
        rightsRevision = getHeadRevision();
        checkRights(null, true);
        return mk.write(in);
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

    private boolean checkPropertyRights(String path) {
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
