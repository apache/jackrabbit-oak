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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.jcr.PropertyType;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexConsistencyChecker {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeState rootState;
    private final String indexPath;

    public enum Level {
        /**
         * Consistency check would only check if all blobs referred by index nodes
         * are present in BlobStore
         */
        BLOBS_ONLY,
        /**
         * Performs full check via {@link org.apache.lucene.index.CheckIndex}. This
         * reads whole index and hence can take time
         */
        FULL
    }

    public static class Result {
        /** True if no problems were found with the index. */
        public boolean clean;

        public boolean typeMismatch;

        public boolean missingBlobs;

        public boolean blobSizeMismatch;

        public String indexPath;

        public long binaryPropSize;

        public List<String> invalidBlobIds = new ArrayList<>();

        public List<String> msgs = new ArrayList<>();
    }

    public IndexConsistencyChecker(NodeState rootState, String indexPath) {
        this.rootState = rootState;
        this.indexPath = indexPath;
    }

    public Result check(Level level){
        Stopwatch watch = Stopwatch.createStarted();
        Result result = new Result();
        result.indexPath = indexPath;
        result.clean = true;

        log.debug("[{}] Starting check", indexPath);

        switch (level){
            case BLOBS_ONLY :
                checkBlobs(result);
                break;
        }

        if (result.clean){
            log.info("[] No problems were detected with this index. Time taken {}", indexPath, watch);
        } else {
            log.info("[] Problems detected with this index. Time taken {}", indexPath, watch);
        }

        return result;
    }

    private void checkBlobs(Result result) {
        Root root = RootFactory.createReadOnlyRoot(rootState);
        Tree idx = root.getTree(indexPath);
        PropertyState type = idx.getProperty("type");
        if (type != null && LuceneIndexConstants.TYPE_LUCENE.equals(type.getValue(Type.STRING))){
            checkBlobs(result, idx);
        } else {
            result.clean = false;
            result.typeMismatch = true;
        }
    }

    private void checkBlobs(Result result, Tree tree) {
        for (PropertyState ps : tree.getProperties()){
            if (ps.getType().tag() == PropertyType.BINARY){
                if (ps.isArray()){
                    for (int i = 0; i < ps.count(); i++) {
                        Blob b = ps.getValue(Type.BINARY, i);
                        checkBlob(ps.getName(), b, tree, result);
                    }
                } else {
                    Blob b = ps.getValue(Type.BINARY);
                    checkBlob(ps.getName(), b, tree, result);
                }
            }
        }

        for (Tree child : tree.getChildren()){
            checkBlobs(result, child);
        }
    }

    private void checkBlob(String propName, Blob blob, Tree tree, Result result) {
        String id = blob.getContentIdentity();
        String blobPath = String.format("%s/%s/%s", tree.getPath(), propName, id);
        try{
            InputStream is = blob.getNewStream();
            CountingInputStream cis = new CountingInputStream(is);
            IOUtils.copyLarge(cis, ByteStreams.nullOutputStream());

            if (cis.getCount() != blob.length()){
                String msg = String.format("Invalid blob %s. Length mismatch - expected ${%d} -> found ${%d}",
                        blobPath, blob.length(), cis.getCount());
                result.msgs.add(msg);
                result.invalidBlobIds.add(id);
                log.warn("[{}] {}", indexPath, msg);
                result.clean = false;
                result.blobSizeMismatch = true;
            }
            result.binaryPropSize += cis.getCount();
        } catch (Exception e) {
            log.warn("[{}] Error occurred reading blob at {}", indexPath, blobPath, e);
            result.invalidBlobIds.add(id);
            result.clean = false;
            result.missingBlobs = true;
        }
    }

}
