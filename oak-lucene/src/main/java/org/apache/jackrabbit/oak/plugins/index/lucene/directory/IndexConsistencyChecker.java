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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import javax.jcr.PropertyType;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexConsistencyChecker {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeState rootState;
    private final String indexPath;
    private final File workDirRoot;
    private File workDir;

    public enum Level {
        /**
         * Consistency check would only check if all blobs referred by index nodes
         * are present in BlobStore
         */
        BLOBS_ONLY,
        /**
         * Performs full check via {@code org.apache.lucene.index.CheckIndex}. This
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

        public List<FileSizeStatus> invalidBlobIds = new ArrayList<>();

        public List<String> missingBlobIds = new ArrayList<>();

        public List<DirectoryStatus> dirStatus = new ArrayList<>();
    }

    public static class DirectoryStatus {
        public final String dirName;

        public final List<String> missingFiles = new ArrayList<>();

        public final List<FileSizeStatus> filesWithSizeMismatch = new ArrayList<>();

        public boolean clean;

        public long size;

        public CheckIndex.Status status;

        public long numDocs;

        public DirectoryStatus(String dirName) {
            this.dirName = dirName;
        }
    }

    public static class FileSizeStatus {
        public final String name;

        public final long actualSize;

        public final long expectedSize;

        public FileSizeStatus(String name, long actualSize, long expectedSize) {
            this.name = name;
            this.actualSize = actualSize;
            this.expectedSize = expectedSize;
        }

        @Override
        public String toString() {
            return String.format("%s => expected %d, actual %d", name, expectedSize, actualSize);
        }
    }

    /**
     * Checks the index at given path for consistency
     *
     * @param rootState root state of repository
     * @param indexPath path of index which needs to be checked
     * @param workDirRoot directory which would be used for copying the index file locally to perform
     *                    check. File would be created in a subdirectory. If the index is valid
     *                    then the files would be removed otherwise whatever files have been copied
     *                    would be left as is
     */
    public IndexConsistencyChecker(NodeState rootState, String indexPath, File workDirRoot) {
        this.rootState = checkNotNull(rootState);
        this.indexPath = checkNotNull(indexPath);
        this.workDirRoot = checkNotNull(workDirRoot);
    }

    public Result check(Level level) throws IOException {
        Stopwatch watch = Stopwatch.createStarted();
        Result result = new Result();
        result.indexPath = indexPath;
        result.clean = true;

        log.debug("[{}] Starting check", indexPath);

        checkBlobs(result);
        if (level == Level.FULL && result.clean){
            checkIndex(result);
        }

        if (result.clean){
            log.info("[] No problems were detected with this index. Time taken {}", indexPath, watch);
            FileUtils.deleteQuietly(workDir);
        } else {
            log.warn("[] Problems detected with this index. Time taken {}", indexPath, watch);
            if (workDir != null) {
                log.warn("[] Index files are copied to {}", indexPath, workDir.getAbsolutePath());
            }
        }
        return result;
    }

    private void checkIndex(Result result) throws IOException {
        NodeState idx = NodeStateUtils.getNode(rootState, indexPath);
        IndexDefinition defn = IndexDefinition.newBuilder(rootState, idx, indexPath).build();
        workDir = createWorkDir(workDirRoot, PathUtils.getName(indexPath));

        for (String dirName : idx.getChildNodeNames()){
            //TODO Check for SuggestionDirectory Pending
            if (NodeStateUtils.isHidden(dirName) && MultiplexersLucene.isIndexDirName(dirName)){
                DirectoryStatus dirStatus = new DirectoryStatus(dirName);
                result.dirStatus.add(dirStatus);
                log.warn("[{}] Checking directory {}", indexPath, dirName);
                try {
                    checkIndexDirectory(dirStatus, idx, defn, workDir, dirName);
                } catch (IOException e){
                    dirStatus.clean = false;
                    log.warn("[{}][{}] Error occurred while performing directory check", indexPath, dirName, e);
                }

                if (!dirStatus.clean){
                    result.clean = false;
                }
            }
        }
    }

    private void checkIndexDirectory(DirectoryStatus dirStatus, NodeState idx, IndexDefinition defn,
                                     File workDir, String dirName) throws IOException {
        File idxDir = createWorkDir(workDir, dirName);
        Directory sourceDir = new OakDirectory(new ReadOnlyBuilder(idx), dirName, defn, true);
        Directory targetDir = FSDirectory.open(idxDir);

        boolean clean = true;
        for (String file : sourceDir.listAll()) {
            log.debug("[{}][{}] Checking {}", indexPath, dirName, file);
            try {
                sourceDir.copy(targetDir, file, file, IOContext.DEFAULT);
            } catch (FileNotFoundException ignore){
                dirStatus.missingFiles.add(file);
                clean = false;
                log.warn("[{}][{}] File {} missing", indexPath, dirName, file);
            }

            if (targetDir.fileLength(file) != sourceDir.fileLength(file)){
                FileSizeStatus fileStatus = new FileSizeStatus(file, targetDir.fileLength(file), sourceDir.fileLength(file));
                dirStatus.filesWithSizeMismatch.add(fileStatus);
                clean = false;
                log.warn("[{}][{}] File size mismatch {}", indexPath, dirName, fileStatus);
            } else {
                dirStatus.size += sourceDir.fileLength(file);
                log.debug("[{}][{}] File {} is consistent", indexPath, dirName, file);
            }
        }

        if (clean){
            log.debug("[{}][{}] Directory content found to be consistent. Proceeding to IndexCheck", indexPath, dirName);
            CheckIndex ci = new CheckIndex(targetDir);

            if (log.isDebugEnabled()) {
                ci.setInfoStream(new LoggingPrintStream(log), log.isTraceEnabled());
            }

            dirStatus.status = ci.checkIndex();
            dirStatus.clean = dirStatus.status.clean;
            log.debug("[{}][{}] IndexCheck was successful. Proceeding to open DirectoryReader", indexPath, dirName);
        }

        if (dirStatus.clean){
            DirectoryReader dirReader = DirectoryReader.open(targetDir);
            dirStatus.numDocs = dirReader.numDocs();
            log.debug("[{}][{}] DirectoryReader can be opened", indexPath, dirName);
            dirReader.close();
        }
    }

    //~---------------------------------------< Blob Validation >

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
                result.invalidBlobIds.add(new FileSizeStatus(blobPath, cis.getCount(), blob.length()));
                log.warn("[{}] {}", indexPath, msg);
                result.clean = false;
                result.blobSizeMismatch = true;
            }
            result.binaryPropSize += cis.getCount();
        } catch (Exception e) {
            log.warn("[{}] Error occurred reading blob at {}", indexPath, blobPath, e);
            result.missingBlobIds.add(id);
            result.clean = false;
            result.missingBlobs = true;
        }
    }

    //~-----------------------------------------------< utility >

    private static File createWorkDir(File parent, String name) throws IOException {
        String fsSafeName = IndexRootDirectory.getFSSafeName(name);
        File dir = new File(parent, fsSafeName);
        FileUtils.forceMkdir(dir);
        FileUtils.cleanDirectory(dir);
        return dir;
    }

    /**
     * Adapter to pipe info messages from lucene into log messages.
     */
    private static final class LoggingPrintStream extends PrintStream {

        /** Buffer print calls until a newline is written */
        private final StringBuffer buffer = new StringBuffer();

        private final Logger log;

        public LoggingPrintStream(Logger log) {
            super(ByteStreams.nullOutputStream());
            this.log = log;
        }

        public void print(String s) {
            buffer.append(s);
        }

        public void println(String s) {
            buffer.append(s);
            log.debug(buffer.toString());
            buffer.setLength(0);
        }
    }
}
