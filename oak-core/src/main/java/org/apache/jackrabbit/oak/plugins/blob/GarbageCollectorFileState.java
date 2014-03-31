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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;

/**
 * Class for keeping the file system state of the garbage collection.
 * 
 * Also, manages any temporary files needed as well as external sorting.
 * 
 */
class GarbageCollectorFileState {

    private static final String GC_DIR = "gc";

    private static final String MARKED_PREFIX = "marked";

    private static final String AVAIL_PREFIX = "avail";

    private static final String GC_CANDIDATE_PREFIX = "gccand";

    private static final String GC_PREFIX = "gc";

    /** The startTime which records the starting time. */
    private long startTime;

    /** The root of the gc file state directory. */
    private File home;

    /** The marked references. */
    private File markedRefs;

    /** The available references. */
    private File availableRefs;

    /** The gc candidates. */
    private File gcCandidates;

    /** The garbage stores the garbage collection candidates which were not deleted . */
    private File garbage;

    /**
     * Instantiates a new garbage collector file state.
     * 
     * @param root
     *            the root
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public GarbageCollectorFileState(String root) throws IOException {
        init(root);
    }

    /**
     * Gets the home directory.
     * 
     * @return the home
     */
    protected File getHome() {
        return home;
    }

    /**
     * Gets the file storing the marked references.
     * 
     * @return the marked references
     */
    protected File getMarkedRefs() {
        return createMarkedRefsFile();
    }

    /**
     * Gets the file storing the available references.
     * 
     * @return the available references
     */
    protected File getAvailableRefs() {
        return createAvailableRefsFile();
    }

    /**
     * Gets the file storing the gc candidates.
     * 
     * @return the gc candidates
     */
    protected File getGcCandidates() {
        return createGcCandidatesFile();
    }

    /**
     * Gets the storing the garbage.
     * 
     * @return the garbage
     */
    protected File getGarbage() {
        return createGarbageFile();
    }

    /**
     * Initialize the state.
     * 
     * @param root
     *            the root
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void init(String root) throws IOException {
        startTime = System.currentTimeMillis();

        home = new File(root, GC_DIR);
        FileUtils.forceMkdir(home);
        home.deleteOnExit();
    }

    /**
     * Creates the marked references file.
     * 
     * @return the file
     */
    private File createMarkedRefsFile() {
        if (markedRefs == null) {
            markedRefs = new File(home,
                    MARKED_PREFIX + "-" + startTime);
            markedRefs.deleteOnExit();
        }
        return markedRefs;
    }

    /**
     * Creates the available references file.
     * 
     * @return the file
     */
    private File createAvailableRefsFile() {
        if (availableRefs == null) {
            availableRefs = new File(home,
                    AVAIL_PREFIX + "-" + startTime);
            availableRefs.deleteOnExit();
        }
        return availableRefs;
    }

    /**
     * Creates the gc candidates file.
     * 
     * @return the file
     */
    private File createGcCandidatesFile() {
        if (gcCandidates == null) {
            gcCandidates = new File(home,
                    GC_CANDIDATE_PREFIX + "-" + startTime);
            gcCandidates.deleteOnExit();
        }
        return gcCandidates;
    }

    /**
     * Creates the garbage file.
     * 
     * @return the file
     */
    private File createGarbageFile() {
        if (garbage == null) {
            garbage = new File(home,
                    GC_PREFIX + "-" + startTime);
            garbage.deleteOnExit();
        }
        return garbage;
    }

    /**
     * Creates a temp file.
     * 
     * @return the file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    protected File createTempFile() throws IOException {
        return File.createTempFile("temp", null, home);
    }

    /**
     * Completes the process by deleting the files.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    protected void complete() throws IOException {
        if (!getGarbage().exists() ||
                FileUtils.sizeOf(getGarbage()) == 0) {
            FileUtils.deleteDirectory(home);
        }
    }

    /**
     * Sorts the given file externally.
     * 
     * @param file
     *            the file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void sort(File file) throws IOException {
        File sorted = createTempFile();
        Comparator<String> lexComparator = new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return s1.compareTo(s2);
            }
        };
        ExternalSort.mergeSortedFiles(
                ExternalSort.sortInBatch(file, lexComparator, true),
                sorted, lexComparator, true);
        Files.move(sorted, file);
    }
}