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
package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.Files.readAllLines;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.WRITE;

public class LocalGCJournalFile implements GCJournalFile {

    private final File file;

    public LocalGCJournalFile(File parent, String name) {
        this(new File(parent, name));
    }

    public LocalGCJournalFile(File file) {
        this.file = file;
    }

    @Override
    public void writeLine(String line) throws IOException {
        try (BufferedWriter w = newBufferedWriter(file.toPath(), UTF_8, WRITE, APPEND, CREATE, DSYNC)) {
            w.write(line);
            w.newLine();
        }
    }

    @Override
    public List<String> readLines() throws IOException {
        if (file.exists()) {
            return readAllLines(file.toPath(), UTF_8);
        }
        return new ArrayList<String>();
    }
}
