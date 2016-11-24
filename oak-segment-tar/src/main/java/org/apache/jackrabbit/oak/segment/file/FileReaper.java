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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.collect.Lists.newArrayList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe class tracking files to be removed.
 */
class FileReaper {

    private static final Logger logger = LoggerFactory.getLogger(FileReaper.class);

    private final Set<File> files = new HashSet<>();

    private final Object lock = new Object();

    /**
     * Add files to be removed. The same file can be added more than once.
     * Duplicates are ignored.
     *
     * @param files group of files to be removed.
     */
    void add(Iterable<File> files) {
        synchronized (lock) {
            for (File file : files) {
                this.files.add(file);
            }
        }
    }

    /**
     * Reap previously added files.
     */
    void reap() {
        Set<File> reap;

        synchronized (lock) {
            reap = new HashSet<>(files);
            files.clear();
        }

        Set<File> redo = new HashSet<>();
        List<File> removed = newArrayList();
        for (File file : reap) {
            try {
                Files.deleteIfExists(file.toPath());
                removed.add(file);
            } catch (IOException e) {
                logger.warn(String.format("Unable to remove file %s", file), e);
                redo.add(file);
            }
        }
        if (!removed.isEmpty()) {
            logger.info("Removed files {}", Joiner.on(",").join(removed));
        }

        if (redo.isEmpty()) {
            return;
        }

        add(redo);
    }

}
