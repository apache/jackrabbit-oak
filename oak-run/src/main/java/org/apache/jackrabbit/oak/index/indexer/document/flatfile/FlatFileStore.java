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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;

public class FlatFileStore implements Iterable<NodeStateEntry>, Closeable{
    private final Closer closer = Closer.create();
    private final File storeFile;
    private final NodeStateEntryReader entryReader;
    private final int checkChildLimit;
    private long entryCount = -1;

    public FlatFileStore(File storeFile, NodeStateEntryReader entryReader, int checkChildLimit) {
        this.storeFile = storeFile;
        this.entryReader = entryReader;
        this.checkChildLimit = checkChildLimit;
    }

    public long getEntryCount() {
        return entryCount;
    }

    public void setEntryCount(long entryCount) {
        this.entryCount = entryCount;
    }

    @Override
    public Iterator<NodeStateEntry> iterator() {
        return new FlatFileStoreIterator(createBaseIterator(), checkChildLimit);
    }

    private Iterator<NodeStateEntry> createBaseIterator() {
        LineIterator itr = new LineIterator(createReader());
        closer.register(itr::close);
        return new AbstractIterator<NodeStateEntry>() {
            @Override
            protected NodeStateEntry computeNext() {
                if (itr.hasNext()) {
                   return convert(itr.nextLine());
                }

                //End of iterator then close it
                LineIterator.closeQuietly(itr);
                return endOfData();
            }
        };
    }

    private NodeStateEntry convert(String line) {
        return entryReader.read(line);
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    private Reader createReader() {
        try {
            return Files.newReader(storeFile, Charsets.UTF_8);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error opening file " + storeFile, e);
        }
    }
}
