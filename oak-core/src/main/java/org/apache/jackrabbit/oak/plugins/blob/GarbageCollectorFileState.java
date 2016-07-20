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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * Class for keeping the file system state of the garbage collection.
 * 
 * Also, manages any temporary files needed as well as external sorting.
 * 
 */
public class GarbageCollectorFileState implements Closeable{
    /** The root of the gc file state directory. */
    private final File home;

    /** The marked references. */
    private final File markedRefs;

    /** The available references. */
    private final File availableRefs;

    /** The gc candidates. */
    private final File gcCandidates;

    /** The garbage stores the garbage collection candidates which were not deleted . */
    private final File garbage;

    /**
     * Instantiates a new garbage collector file state.
     * 
     * @param root path of the root directory under which the
     *             files created during gc are stored
     */
    public GarbageCollectorFileState(String root) throws IOException {
        long startTime = System.currentTimeMillis();
        home = new File(root, "gcworkdir-" + startTime);
        markedRefs = new File(home, "marked-" + startTime);
        availableRefs = new File(home,"avail-" + startTime);
        gcCandidates = new File(home, "gccand-" + startTime);
        garbage = new File(home, "gc-" + startTime);
        FileUtils.forceMkdir(home);
    }

    /**
     * Gets the file storing the marked references.
     * 
     * @return the marked references
     */
    public File getMarkedRefs() {
        return markedRefs;
    }

    /**
     * Gets the file storing the available references.
     * 
     * @return the available references
     */
    public File getAvailableRefs() {
        return availableRefs;
    }

    /**
     * Gets the file storing the gc candidates.
     * 
     * @return the gc candidates
     */
    public File getGcCandidates() {
        return gcCandidates;
    }

    /**
     * Gets the storing the garbage.
     * 
     * @return the garbage
     */
    public File getGarbage() {
        return garbage;
    }

    /**
     * Completes the process by deleting the files.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void close() throws IOException {
        if (!getGarbage().exists() ||
                FileUtils.sizeOf(getGarbage()) == 0) {
            FileUtils.deleteDirectory(home);
        }
    }
}
