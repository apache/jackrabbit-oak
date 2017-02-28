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

import static java.nio.charset.Charset.defaultCharset;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.AbstractIterator;

/**
 * Iterator over the revisions in the journal in reverse order
 * (end of the file to beginning).
 */
public final class JournalReader extends AbstractIterator<JournalEntry> implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JournalReader.class);

    private final ReversedLinesFileReader journal;

    public JournalReader(File journalFile) throws IOException {
        journal = new ReversedLinesFileReader(journalFile, defaultCharset());
    }

    /**
     * @throws IllegalStateException  if an {@code IOException} occurs while reading from
     *                                the journal file.
     */
    @Override
    protected JournalEntry computeNext() {
        try {
            String line = null;
            while ((line = journal.readLine()) != null) {
                if (line.indexOf(' ') != -1) {
                    List<String> splits = Splitter.on(' ').splitToList(line);
                    String revision = splits.get(0);
                    long timestamp = -1L;
                    
                    if (splits.size() > 2) {
                        try {
                            timestamp = Long.parseLong(splits.get(2));
                        } catch (NumberFormatException e) {
                            LOG.warn("Ignoring malformed timestamp {} for revision {}", splits.get(2), revision);
                        }
                    } else {
                        LOG.warn("Timestamp information is missing for revision {}", revision);
                    }

                    return new JournalEntry(revision, timestamp);
                } else {
                    LOG.warn("Skipping invalid journal entry: {}", line);
                }
            }
        } catch (IOException e) {
            LOG.error("Error reading journal file", e);
        }
        return endOfData();
    }

    @Override
    public void close() throws IOException {
        journal.close();
    }
}
