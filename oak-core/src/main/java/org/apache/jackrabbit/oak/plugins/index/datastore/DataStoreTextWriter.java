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

package org.apache.jackrabbit.oak.plugins.index.datastore;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.InMemoryDataRecord;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TextWriter;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText.ExtractionResult;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * TextWriter implementation which just stores the extracted text
 * as files using the same layout as used by FileDataStore
 */
public class DataStoreTextWriter implements TextWriter, Closeable, PreExtractedTextProvider {
    private static final String ERROR_BLOB_FILE = "blobs_error.txt";
    private static final String EMPTY_BLOB_FILE = "blobs_empty.txt";

    private static final Logger log = LoggerFactory.getLogger(DataStoreTextWriter.class);
    private File directory;

    private final SetHolder emptyBlobsHolder;
    private final SetHolder errorBlobsHolder;
    private boolean closed;
    /**
     * Flag indicating that blobId passed is one from DataStoreBlobStore
     * As those blobId's have the length encoded which would need to be
     * stripped of
     */
    private boolean dataStoreBlobId = true;

    private final boolean readOnlyMode;

    public DataStoreTextWriter(File directory, boolean readOnlyMode) throws IOException {
        if (!directory.exists()) {
            checkArgument(directory.mkdirs(), "Cannot create directory %s", directory.getAbsolutePath());
        }
        this.directory = directory;
        this.readOnlyMode = readOnlyMode;
        this.emptyBlobsHolder = new SetHolder(createLoader(EMPTY_BLOB_FILE), readOnlyMode);
        this.errorBlobsHolder = new SetHolder(createLoader(ERROR_BLOB_FILE), readOnlyMode);

        if (!readOnlyMode) {
            log.info("Using {} to store the extracted text content. Empty count {}, Error count {}",
                    directory.getAbsolutePath(), getEmptyBlobs().size(), getErrorBlobs().size());
        } else {
            log.info("Using extracted store from {}", directory.getAbsolutePath());
        }
    }

    @Override
    public ExtractedText getText(String propertyPath, Blob blob) throws IOException {
        String blobId = blob.getContentIdentity();
        if (blobId == null) {
            log.debug("No id found for blob at path {}", propertyPath);
            return null;
        }

        blobId = stripLength(blobId);
        //Check for ref being non null to ensure its not an inlined binary
        if (InMemoryDataRecord.isInstance(blobId)) {
            log.debug("Pre extraction is not supported for in memory records. Path {}, BlobId {}", propertyPath, blobId);
            return null;
        }

        ExtractedText result = null;
        if (getEmptyBlobs().contains(blobId)) {
            result = ExtractedText.EMPTY;
        } else if (getErrorBlobs().contains(blobId)) {
            result = ExtractedText.ERROR;
        } else {
            File textFile = getFile(blobId);
            if (textFile.exists()) {
                String text = Files.toString(textFile, Charsets.UTF_8);
                result = new ExtractedText(ExtractionResult.SUCCESS, text);
            }
        }

        if (log.isDebugEnabled()){
            String extractionResult = result != null ? result.getExtractionResult().toString() : null;
            log.debug("Extraction result for [{}] at path [{}] is [{}]", blobId, propertyPath, extractionResult);
        }
        return result;
    }

    @Override
    public void write(@Nonnull String blobId,@Nonnull String text) throws IOException {
        checkIfReadOnlyModeEnabled();
        checkNotNull(blobId, "BlobId cannot be null");
        checkNotNull(text, "Text passed for [%s] was null", blobId);

        File textFile = getFile(stripLength(blobId));
        ensureParentExists(textFile);
        //TODO should we compress
        Files.write(text, textFile, Charsets.UTF_8);
    }

    @Override
    public synchronized void markEmpty(String blobId) {
        checkIfReadOnlyModeEnabled();
        getEmptyBlobs().add(stripLength(blobId));
    }

    @Override
    public synchronized void markError(String blobId) {
        checkIfReadOnlyModeEnabled();
        getErrorBlobs().add(stripLength(blobId));
    }

    @Override
    public synchronized boolean isProcessed(String blobId) {
        blobId = stripLength(blobId);
        if (getEmptyBlobs().contains(blobId) || getErrorBlobs().contains(blobId)) {
            return true;
        }
        File textFile = getFile(blobId);
        return textFile.exists();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed || readOnlyMode) {
            return;
        }
        writeToFile(EMPTY_BLOB_FILE, getEmptyBlobs());
        writeToFile(ERROR_BLOB_FILE, getErrorBlobs());
        closed = true;
    }

    @Override
    public String toString() {
        return "FileDataStore based text provider";
    }

    SetHolder getEmptyBlobsHolder(){
        return emptyBlobsHolder;
    }

    SetHolder getErrorBlobsHolder() {
        return errorBlobsHolder;
    }

    /**
     * Returns the identified file. This method implements the pattern
     * used to avoid problems with too many files in a single directory.
     * <p/>
     * No sanity checks are performed on the given identifier.
     *
     * @param identifier file name
     * @return identified file
     */
    private File getFile(String identifier) {
        File file = directory;
        file = new File(file, identifier.substring(0, 2));
        file = new File(file, identifier.substring(2, 4));
        file = new File(file, identifier.substring(4, 6));
        return new File(file, identifier);
    }

    private String stripLength(String blobId) {
        if (dataStoreBlobId) {
            return DataStoreBlobStore.BlobId.of(blobId).getBlobId();
        }
        return blobId;
    }

    private Set<String> getEmptyBlobs() {
        return emptyBlobsHolder.get();
    }

    private Set<String> getErrorBlobs() {
        return errorBlobsHolder.get();
    }

    private void checkIfReadOnlyModeEnabled() {
        checkState(!readOnlyMode, "Read only mode enabled");
    }

    private Callable<Set<String>> createLoader(final String fileName) {
        final File file = new File(directory, fileName);
        return new Callable<Set<String>>() {
            @Override
            public Set<String> call() throws Exception {
                return loadFromFile(file);
            }

            @Override
            public String toString() {
                return "Loading state from " + file.getAbsolutePath();
            }
        };
    }

    private Set<String> loadFromFile(File file) throws IOException {
        Set<String> result = Sets.newHashSet();
        if (file.exists()) {
            result.addAll(Files.readLines(file, Charsets.UTF_8));
        }
        return result;
    }

    private void writeToFile(String fileName, Set<String> blobIds) throws IOException {
        if (blobIds.isEmpty()){
            return;
        }
        File file = new File(directory, fileName);
        BufferedWriter bw = Files.newWriter(file, Charsets.UTF_8);
        for (String id : blobIds) {
            bw.write(id);
            bw.newLine();
        }
        bw.close();
    }

    private static void ensureParentExists(File file) throws IOException {
        if (!file.exists()) {
            File parent = file.getParentFile();
            FileUtils.forceMkdir(parent);
        }
    }



    /**
     * While running in read only mode the PreExtractedTextProvider
     * would only be used while reindexing. So as to avoid holding memory
     * SoftReference would be used
     */
    static class SetHolder {
        private final Set<String> state;
        private SoftReference<Set<String>> stateRef;
        private final Callable<Set<String>> loader;
        private int loadCount;

        public SetHolder(Callable<Set<String>> loader, boolean softRef) {
            this.loader = loader;
            if (softRef) {
                this.state = null;
            } else {
                this.state = load();
            }
        }

        public Set<String> get() {
            Set<String> result = state;
            if (result != null) {
                return result;
            }

            if (stateRef != null) {
                result = stateRef.get();
            }

            if (result == null) {
                result = load();
                stateRef = new SoftReference<Set<String>>(result);
            }

            return result;
        }

        public int getLoadCount() {
            return loadCount;
        }

        private Set<String> load() {
            try {
                loadCount++;
                return loader.call();
            } catch (Exception e) {
                log.warn("Error occurred while loading the state via {}", loader, e);
                return Sets.newHashSet();
            }
        }
    }
}
