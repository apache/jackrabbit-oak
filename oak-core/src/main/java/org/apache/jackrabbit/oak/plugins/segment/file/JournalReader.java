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

package org.apache.jackrabbit.oak.plugins.segment.file;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader for journal files of the SegmentMK.
 */
public final class JournalReader implements Closeable, Iterable<String> {
    private static final Logger LOG = LoggerFactory.getLogger(JournalReader.class);

    private final ReversedLinesFileReader journal;

    public JournalReader(File journalFile) throws IOException {
        journal = new ReversedLinesFileReader(journalFile);
    }

    /**
     * @return Iterator over the revisions in the journal in reverse order
     *         (end of the file to beginning).
     */
    @Override
    public Iterator<String> iterator() {
        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                try {
                    String line = journal.readLine();
                    while (line != null) {
                        int space = line.indexOf(' ');
                        if (space != -1) {
                            return line.substring(0, space);
                        }
                        line = journal.readLine();
                    }
                } catch (IOException e) {
                    LOG.error("Error reading journal file", e);
                }
                return endOfData();
            }
        };
    }

    @Override
    public void close() throws IOException {
        journal.close();
    }

}
