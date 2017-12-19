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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.ImmutableList.copyOf;
import static org.apache.commons.io.FileUtils.ONE_GB;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter.getPath;

public class NodeStateEntrySorter {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final int DEFAULTMAXTEMPFILES = 1024;
    private final File nodeStateFile;
    private final File workDir;
    private final Charset charset = UTF_8;
    private final Comparator<Iterable<String>> pathComparator;
    private File sortedFile;
    private boolean useZip;
    private boolean deleteOriginal;
    private long maxMemory = ONE_GB * 5;

    public NodeStateEntrySorter(Comparator<Iterable<String>> pathComparator, File nodeStateFile, File workDir) {
        this(pathComparator, nodeStateFile, workDir, getSortedFileName(nodeStateFile));
    }

    public NodeStateEntrySorter(Comparator<Iterable<String>> pathComparator, File nodeStateFile, File workDir, File sortedFile) {
        this.nodeStateFile = nodeStateFile;
        this.workDir = workDir;
        this.sortedFile = sortedFile;
        this.pathComparator = pathComparator;
    }

    public void setUseZip(boolean useZip) {
        this.useZip = useZip;
    }

    public void setDeleteOriginal(boolean deleteOriginal) {
        this.deleteOriginal = deleteOriginal;
    }

    public void setMaxMemoryInGB(long maxMemoryInGb) {
        this.maxMemory = maxMemoryInGb * ONE_GB;
    }

    public void sort() throws IOException {
        long estimatedMemory = estimateAvailableMemory();
        long memory = Math.min(estimatedMemory, maxMemory);
        log.info("Sorting with memory {} (estimated {})", humanReadableByteCount(memory), humanReadableByteCount(estimatedMemory));
        Stopwatch w = Stopwatch.createStarted();

        Comparator<NodeStateEntryHolder> comparator = Comparator.naturalOrder();
        Function<String, NodeStateEntryHolder> func1 = (line) -> line == null ? null : new NodeStateEntryHolder(line, pathComparator);
        Function<NodeStateEntryHolder, String> func2 = holder -> holder == null ? null : holder.getLine();

        List<File> sortedFiles = ExternalSort.sortInBatch(nodeStateFile,
                comparator, //Comparator to use
                DEFAULTMAXTEMPFILES,
                memory,
                charset, //charset
                workDir,  //temp directory where intermediate files are created
                true,
                0,
                useZip,
                func2,
                func1
        );

        log.info("Batch sorting done in {} with {} files of size {} to merge", w, sortedFiles.size(),
                humanReadableByteCount(sizeOf(sortedFiles)));

        if (deleteOriginal) {
            log.info("Removing the original file {}", nodeStateFile.getAbsolutePath());
            FileUtils.forceDelete(nodeStateFile);
        }

        Stopwatch w2 = Stopwatch.createStarted();

        ExternalSort.mergeSortedFiles(sortedFiles,
                sortedFile,
                comparator,
                charset,
                true,
                false,
                useZip,
                func2,
                func1

        );

        log.info("Merging of sorted files completed in {}", w2);
        log.info("Sorting completed in {}", w);
    }

    public File getSortedFile() {
        return sortedFile;
    }

    private static File getSortedFileName(File file) {
        String extension = FilenameUtils.getExtension(file.getName());
        String baseName = FilenameUtils.getBaseName(file.getName());
        return new File(file.getParentFile(), baseName + "-sorted." + extension);
    }

    private static long sizeOf(List<File> sortedFiles) {
        return sortedFiles.stream().mapToLong(File::length).sum();
    }

    /**
     * This method calls the garbage collector and then returns the free
     * memory. This avoids problems with applications where the GC hasn't
     * reclaimed memory and reports no available memory.
     *
     * @return available memory
     */
    private static long estimateAvailableMemory() {
        System.gc();
        // http://stackoverflow.com/questions/12807797/java-get-available-memory
        Runtime r = Runtime.getRuntime();
        long allocatedMemory = r.totalMemory() - r.freeMemory();
        long presFreeMemory = r.maxMemory() - allocatedMemory;
        return presFreeMemory;
    }

    static class NodeStateEntryHolder implements Comparable<NodeStateEntryHolder> {
        final String line;
        final List<String> pathElements;
        final Comparator<Iterable<String>> comparator;

        public NodeStateEntryHolder(String line, Comparator<Iterable<String>> comparator) {
            this.line = line;
            this.comparator = comparator;
            this.pathElements = copyOf(elements(getPath(line)));
        }

        public String getLine() {
            return line;
        }

        @Override
        public int compareTo(NodeStateEntryHolder o) {
            return comparator.compare(this.pathElements, o.pathElements);
        }
    }

}
