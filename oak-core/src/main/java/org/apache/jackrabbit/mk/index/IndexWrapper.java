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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.mk.ExceptionFactory;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.wrapper.MicroKernelWrapper;
import org.apache.jackrabbit.mk.wrapper.MicroKernelWrapperBase;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.Indexer;
import org.apache.jackrabbit.oak.plugins.index.PrefixIndex;
import org.apache.jackrabbit.oak.plugins.index.PropertyIndex;
import org.apache.jackrabbit.oak.plugins.index.PropertyIndexConstants;

/**
 * The index mechanism, as a wrapper.
 * 
 * @deprecated - see OAK-298
 */
public class IndexWrapper extends MicroKernelWrapperBase implements MicroKernel {

    private final MicroKernelWrapper mk;
    private final Indexer indexer;
    private final Set<String> branchRevisions = Collections.synchronizedSet(new HashSet<String>());

    public IndexWrapper(MicroKernel mk) {
        this.mk = MicroKernelWrapperBase.wrap(mk);
        this.indexer = new Indexer(mk);
        indexer.init();
    }

    public Indexer getIndexer() {
        return indexer;
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
        String indexRoot = indexer.getIndexRootNode();
        if (path.startsWith(indexRoot)) {
            return false;
        }
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
        String branchRevision = mk.branch(trunkRevisionId);
        branchRevisions.add(branchRevision);
        return branchRevision;
    }

    @Override
    public String merge(String branchRevisionId, String message) {
        String headRevision = mk.merge(branchRevisionId, message);
        branchRevisions.remove(branchRevisionId);
        indexer.updateUntil(headRevision);
        return mk.getHeadRevision();
    }

    @Override
    public String commitStream(String rootPath, JsopReader jsonDiff, String revisionId, String message) {
        if (branchRevisions.remove(revisionId)) {
            // TODO update the index in the branch as well, if required
            String rev = mk.commitStream(rootPath, jsonDiff, revisionId, message);
            branchRevisions.add(rev);
            return rev;
        }
        String rev = mk.commitStream(rootPath, jsonDiff, revisionId, message);
        jsonDiff.resetReader();
        indexer.updateIndex(rootPath, jsonDiff, rev);
        rev = mk.getHeadRevision();
        rev = indexer.updateEnd(rev);
        return rev;
    }

    @Override
    public JsopReader getNodesStream(String path, String revisionId, int depth, long offset, int count, String filter) {
        String indexRoot = indexer.getIndexRootNode();
        if (!path.startsWith(indexRoot)) {
            return mk.getNodesStream(path, revisionId, depth, offset, count, filter);
        }
        String index = PathUtils.relativize(indexRoot, path);
        int idx = index.indexOf('?');
        if (idx < 0) {
            // not a query (expected: /index/prefix:x?y) - treat as regular node lookup
            return mk.getNodesStream(path, revisionId, depth, offset, count, filter);
        }
        String data = index.substring(idx + 1);
        index = index.substring(0, idx);
        JsopStream s = new JsopStream();
        s.array();
        if (index.startsWith(PropertyIndexConstants.TYPE_PREFIX)) {
            String prefix = index.substring(PropertyIndexConstants.TYPE_PREFIX.length());
            PrefixIndex prefixIndex = indexer.getPrefixIndex(prefix);
            if (prefixIndex == null) {
                throw ExceptionFactory.get("Unknown index: " + index);
            }
            Iterator<String> it = prefixIndex.getPaths(data, revisionId);
            while (it.hasNext()) {
                s.value(it.next());
            }
        } else if (index.startsWith(PropertyIndexConstants.TYPE_PROPERTY)) {
            String property = index.substring(PropertyIndexConstants.TYPE_PROPERTY.length());
            boolean unique = false;
            if (property.endsWith("," + PropertyIndexConstants.UNIQUE)) {
                unique = true;
                property = property.substring(0, property.length() - PropertyIndexConstants.UNIQUE.length() - 1);
            }
            PropertyIndex propertyIndex = indexer.getPropertyIndex(property);
            if (propertyIndex == null) {
                throw ExceptionFactory.get("Unknown index: " + index);
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
    public JsopReader diffStream(String fromRevisionId, String toRevisionId, String path, int depth) {
        return mk.diffStream(fromRevisionId, toRevisionId, path, depth);
    }

    @Override
    public JsopReader getJournalStream(String fromRevisionId, String toRevisionId, String path) {
        return mk.getJournalStream(fromRevisionId, toRevisionId, path);
    }

    @Override
    public JsopReader getRevisionsStream(long since, int maxEntries, String path) {
        return mk.getRevisionsStream(since, maxEntries, path);
    }

    public void dispose() {
        // do nothing
    }

    public MicroKernel getBaseKernel() {
        return mk;
    }

}
