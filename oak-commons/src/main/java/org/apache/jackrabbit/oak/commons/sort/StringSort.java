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

package org.apache.jackrabbit.oak.commons.sort;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.escapeLineBreak;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.unescapeLineBreaks;

/**
 * Utility class to store a list of string and perform sort on that. For small size
 * the list would be maintained in memory. If the size crosses the required threshold then
 * the sorting would be performed externally
 */
public class StringSort implements Iterable<String>, Closeable {
    private final Logger log = LoggerFactory.getLogger(getClass());
    public static final int BATCH_SIZE = 2048;

    private final int overflowToDiskThreshold;
    private final Comparator<String> comparator;

    private final List<String> ids = Lists.newArrayList();
    private long size;

    private final List<String> inMemBatch = Lists.newArrayList();

    private boolean useFile;
    private PersistentState persistentState;

    public StringSort(int overflowToDiskThreshold, Comparator<String> comparator) {
        this.overflowToDiskThreshold = overflowToDiskThreshold;
        this.comparator = comparator;
    }

    public void add(String id) throws IOException {
        if (useFile) {
            addToBatch(id);
        } else {
            ids.add(id);
            if (ids.size() >= overflowToDiskThreshold) {
                flushToFile(ids);
                useFile = true;
                log.debug("In memory buffer crossed the threshold of {}. " +
                        "Switching to filesystem [{}] to manage the state", overflowToDiskThreshold, persistentState);
            }
        }
        size++;
    }

    public void sort() throws IOException {
        if (useFile) {
            //Flush the last batch
            flushToFile(inMemBatch);
            persistentState.sort();
        } else {
            Collections.sort(ids, comparator);
        }
    }

    public Iterator<String> getIds() throws IOException {
        if (useFile) {
            return persistentState.getIterator();
        } else {
            return ids.iterator();
        }
    }

    public long getSize() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean usingFile() {
        return useFile;
    }

    @Override
    public void close() throws IOException {
        if (persistentState != null) {
            persistentState.close();
        }
    }

    @Override
    public Iterator<String> iterator() {
        try {
            return getIds();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    //--------------------------< internal >------------------------------------

    private void addToBatch(String id) throws IOException {
        inMemBatch.add(id);
        if (inMemBatch.size() >= BATCH_SIZE) {
            flushToFile(inMemBatch);
        }
    }

    private void flushToFile(List<String> ids) throws IOException {
        BufferedWriter w = getPersistentState().getWriter();
        for (String id : ids) {
            w.write(escapeLineBreak(id));
            w.newLine();
        }
        ids.clear();
    }

    private PersistentState getPersistentState() {
        //Lazily initialize the persistent state
        if (persistentState == null) {
            persistentState = new PersistentState(comparator);
        }
        return persistentState;
    }

    private static class PersistentState implements Closeable {
        /**
         * Maximum loop count when creating temp directories.
         */
        private static final int TEMP_DIR_ATTEMPTS = 10000;

        private final Charset charset = Charsets.UTF_8;
        private final File workDir;
        private final Comparator<String> comparator;
        private File idFile;
        private File sortedFile;
        private BufferedWriter writer;
        private List<CloseableIterator> openedIterators = Lists.newArrayList();

        public PersistentState(Comparator<String> comparator) {
            this(comparator, createTempDir("oak-sorter-"));
        }

        public PersistentState(Comparator<String> comparator, File workDir) {
            this.workDir = workDir;
            this.comparator = FileIOUtils.lineBreakAwareComparator(comparator);
        }

        public BufferedWriter getWriter() throws FileNotFoundException {
            if (idFile == null) {
                idFile = new File(workDir, "strings.txt");
                sortedFile = new File(workDir, "strings-sorted.txt");
                writer = Files.newWriter(idFile, charset);
            }
            return writer;
        }

        public void sort() throws IOException {
            closeWriter();

            List<File> sortedFiles = ExternalSort.sortInBatch(idFile,
                    comparator, //Comparator to use
                    ExternalSort.DEFAULTMAXTEMPFILES,
                    ExternalSort.DEFAULT_MAX_MEM_BYTES,
                    charset, //charset
                    workDir,  //temp directory where intermediate files are created
                    true //distinct
            );

            ExternalSort.mergeSortedFiles(sortedFiles,
                    sortedFile,
                    comparator,
                    charset,
                    true
            );
        }

        public Iterator<String> getIterator() throws IOException {
            CloseableIterator itr = new CloseableIterator(Files.newReader(sortedFile, charset));
            openedIterators.add(itr);
            return itr;
        }

        @Override
        public String toString() {
            return "PersistentState : workDir=" + workDir.getAbsolutePath();
        }

        @Override
        public void close() throws IOException {
            Closer closer = Closer.create();
            try {
                //Closing is done in LIFO manner!
                closer.register(new Closeable() {
                    @Override
                    public void close() throws IOException {
                        FileUtils.deleteDirectory(workDir);
                    }
                });
                closer.register(writer);
                for (CloseableIterator citr : openedIterators) {
                    closer.register(citr);
                }
            } finally {
                closer.close();
            }
        }

        private void closeWriter() throws IOException {
            writer.close();
        }

        /**
         * Taken from com.google.common.io.Files#createTempDir()
         * Modified to provide a prefix
         */
        private static File createTempDir(String prefix) {
            File baseDir = new File(System.getProperty("java.io.tmpdir"));
            String baseName = System.currentTimeMillis() + "-";

            for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
                File tempDir = new File(baseDir, prefix + baseName + counter);
                if (tempDir.mkdir()) {
                    return tempDir;
                }
            }
            throw new IllegalStateException("Failed to create directory within "
                    + TEMP_DIR_ATTEMPTS + " attempts (tried "
                    + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
        }
    }

    private static class CloseableIterator extends LineIterator implements Closeable {
        public CloseableIterator(Reader reader) throws IllegalArgumentException {
            super(reader);
        }

        @Override
        public String next() {
            return unescapeLineBreaks(super.next());
        }
    }
}
