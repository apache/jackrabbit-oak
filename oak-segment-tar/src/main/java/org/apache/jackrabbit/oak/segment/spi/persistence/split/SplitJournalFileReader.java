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
package org.apache.jackrabbit.oak.segment.spi.persistence.split;

import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;

import java.io.IOException;
import java.util.Optional;

public class SplitJournalFileReader implements JournalFileReader {

    private final JournalFileReader roJournalReader;

    private final JournalFileReader rwJournalReader;

    private final Optional<String> lastRoJournalEntry;

    private boolean rwJournalReaderHasFinished;

    private boolean roJournalReaderHasFinished;

    public SplitJournalFileReader(JournalFileReader roJournalReader, JournalFileReader rwJournalReader, Optional<String> lastRoJournalEntry) {
        this.roJournalReader = roJournalReader;
        this.rwJournalReader = rwJournalReader;
        this.lastRoJournalEntry = lastRoJournalEntry;
    }

    @Override
    public String readLine() throws IOException {
        if (!rwJournalReaderHasFinished) {
            String line = rwJournalReader.readLine();
            if (line != null) {
                return line;
            }
            rwJournalReaderHasFinished = true;
            if (lastRoJournalEntry.isPresent()) {
                rewindToLine(roJournalReader, lastRoJournalEntry.get());
                return lastRoJournalEntry.get();
            } else {
                roJournalReaderHasFinished = true;
            }
        }
        if (!roJournalReaderHasFinished) {
            String line = roJournalReader.readLine();
            if (line != null) {
                return line;
            }
            roJournalReaderHasFinished = true;
        }
        return null;
    }

    private void rewindToLine(JournalFileReader reader, String stopLine) throws IOException {
        while (true) {
            String line = reader.readLine();
            if (line == null || line.equals(stopLine)) {
                break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        roJournalReader.close();
        rwJournalReader.close();
    }
}
