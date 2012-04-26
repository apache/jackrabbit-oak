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
package org.apache.jackrabbit.mk.index;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.mk.simple.NodeMap;
import org.apache.jackrabbit.mk.util.ExceptionFactory;
import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.mk.wrapper.MicroKernelWrapper;
import org.apache.jackrabbit.mk.wrapper.MicroKernelWrapperBase;

/**
 * The index mechanism, as a wrapper.
 */
public class IndexWrapper extends MicroKernelWrapperBase implements MicroKernel {

    private static final String INDEX_PATH = "/index";
    private static final String TYPE_PREFIX = "prefix:";
    private static final String TYPE_PROPERTY = "property:";
    private static final String UNIQUE = "unique";

    private final MicroKernelWrapper mk;
    private final Indexer indexer;
    private final NodeMap map = new NodeMap();
    private final HashMap<String, PrefixIndex> prefixIndexes = new HashMap<String, PrefixIndex>();
    private final HashMap<String, PropertyIndex> propertyIndexes = new HashMap<String, PropertyIndex>();

    public IndexWrapper(MicroKernel mk) {
        this.mk = MicroKernelWrapperBase.wrap(mk);
        this.indexer = new Indexer(mk);
        indexer.init();
    }

    @Override
    public String getHeadRevision() {
        return mk.getHeadRevision();
    }

    @Override
    public long getLength(String blobId) {
        return mk.getLength(blobId);
    }

    @Override
    public boolean nodeExists(String path, String revisionId) {
        return mk.nodeExists(path, revisionId);
    }

    @Override
    public long getChildNodeCount(String path, String revisionId) {
        return mk.getChildNodeCount(path, revisionId);
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length) {
        return mk.read(blobId, pos, buff, off, length);
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long maxWaitMillis) throws MicroKernelException, InterruptedException {
        return mk.waitForCommit(oldHeadRevisionId, maxWaitMillis);
    }

    @Override
    public String write(InputStream in) {
        return mk.write(in);
    }

    @Override
    public String branch(String trunkRevisionId) {
        return mk.branch(trunkRevisionId);
    }

    @Override
    public String merge(String branchRevisionId, String message) {
        return mk.merge(branchRevisionId, message);
    }

    @Override
    public String commitStream(String rootPath, JsopReader jsonDiff, String revisionId, String message) {
        if (!rootPath.startsWith(INDEX_PATH)) {
            String rev = mk.commitStream(rootPath, jsonDiff, revisionId, message);
            jsonDiff.resetReader();
            indexer.updateIndex(rootPath, jsonDiff, rev);
            rev = mk.getHeadRevision();
            rev = indexer.updateEnd(rev);
            return rev;
        }
        JsopReader t = jsonDiff;
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
                t.read('{');
                // parse but ignore
                NodeImpl.parse(map, t, 0);
                path = PathUtils.relativize(INDEX_PATH, path);
                if (path.startsWith(TYPE_PREFIX)) {
                    String prefix = path.substring(TYPE_PREFIX.length());
                    PrefixIndex idx = indexer.createPrefixIndex(prefix);
                    prefixIndexes.put(path, idx);
                } else if (path.startsWith(TYPE_PROPERTY)) {
                    String property = path.substring(TYPE_PROPERTY.length());
                    boolean unique = false;
                    if (property.endsWith("," + UNIQUE)) {
                        unique = true;
                        property = property.substring(0, property.length() - UNIQUE.length() - 1);
                    }
                    PropertyIndex idx = indexer.createPropertyIndex(property, unique);
                    propertyIndexes.put(path, idx);
                } else {
                    throw ExceptionFactory.get("Unknown index type: " + path);
                }
                break;
            case '-':
                throw ExceptionFactory.get("Removing indexes is not yet implemented");
            default:
                throw ExceptionFactory.get("token: " + (char) t.getTokenType());
            }
        }
        return null;
    }

    @Override
    public JsopReader getNodesStream(String path, String revisionId, int depth, long offset, int count, String filter) {
        if (!path.startsWith(INDEX_PATH)) {
            return mk.getNodesStream(path, revisionId, depth, offset, count, filter);
        }
        String index = PathUtils.relativize(INDEX_PATH, path);
        int idx = index.indexOf('?');
        if (idx < 0) {
            // invalid query - expected: /index/prefix:x?y
            return null;
        }
        String data = index.substring(idx + 1);
        index = index.substring(0, idx);
        JsopStream s = new JsopStream();
        s.array();
        if (index.startsWith(TYPE_PREFIX)) {
            PrefixIndex prefixIndex = prefixIndexes.get(index);
            if (prefixIndex == null) {
                if (mk.nodeExists(path, mk.getHeadRevision())) {
                    prefixIndex = indexer.createPrefixIndex(index);
                } else {
                    throw ExceptionFactory.get("Unknown index: " + index);
                }
            }
            Iterator<String> it = prefixIndex.getPaths(data, revisionId);
            while (it.hasNext()) {
                s.value(it.next());
            }
        } else if (index.startsWith(TYPE_PROPERTY)) {
            PropertyIndex propertyIndex = propertyIndexes.get(index);
            boolean unique = index.endsWith("," + UNIQUE);
            if (propertyIndex == null) {
                if (mk.nodeExists(path, mk.getHeadRevision())) {
                    String indexName = index;
                    if (unique) {
                        indexName = index.substring(0, index.length() - UNIQUE.length() - 1);
                    }
                    propertyIndex = indexer.createPropertyIndex(indexName, unique);
                } else {
                    throw ExceptionFactory.get("Unknown index: " + index);
                }
            }
            if (unique) {
                String value = propertyIndex.getPath(data, revisionId);
                if (value != null) {
                    s.value(value);
                }
            } else {
                Iterator<String> it = propertyIndex.getPaths(data, revisionId);
                while (it.hasNext()) {
                    s.value(it.next());
                }
            }
        }
        s.endArray();
        return s;
    }

    @Override
    public JsopReader diffStream(String fromRevisionId, String toRevisionId, String filter) {
        return mk.diffStream(fromRevisionId, toRevisionId, filter);
    }

    @Override
    public JsopReader getJournalStream(String fromRevisionId, String toRevisionId, String filter) {
        return mk.getJournalStream(fromRevisionId, toRevisionId, filter);
    }

    @Override
    public JsopReader getNodesStream(String path, String revisionId) {
        return getNodesStream(path, revisionId, 1, 0, -1, null);
    }

    @Override
    public JsopReader getRevisionsStream(long since, int maxEntries) {
        return mk.getRevisionsStream(since, maxEntries);
    }

    @Override
    public void dispose() {
        mk.dispose();
    }

}
