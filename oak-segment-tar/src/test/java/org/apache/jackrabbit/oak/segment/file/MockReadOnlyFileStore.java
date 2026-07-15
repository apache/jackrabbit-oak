/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.segment.file;

import java.io.File;
import java.io.IOException;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

public class MockReadOnlyFileStore extends ReadOnlyFileStore {
    private int failAfterReadSegmentCount;
    private int readSegmentCount = 0;

    @NotNull
    public static MockReadOnlyFileStore buildMock(File path, File journalFile) throws InvalidFileStoreVersionException, IOException {
        TarPersistence persistence = new TarPersistence(path, journalFile);
        ReadOnlyRevisions revisions = new ReadOnlyRevisions(persistence);
        MockReadOnlyFileStore store;
        try {
            store = new MockReadOnlyFileStore(fileStoreBuilder(path).withCustomPersistence(persistence));
        } catch (InvalidFileStoreVersionException | IOException e) {
            try {
                revisions.close();
            } catch (IOException re) {
                //ignore
            }
            throw e;
        }
        store.bind(revisions);
        return store;
    }
    
    
    MockReadOnlyFileStore(FileStoreBuilder builder) throws InvalidFileStoreVersionException, IOException {
        super(builder);
    }

    public void failAfterReadSegmentCount(int count) {
        this.failAfterReadSegmentCount = count;
    }
    
    @Override
    public @NotNull Segment readSegment(SegmentId id) {
        readSegmentCount++;
        if (readSegmentCount > failAfterReadSegmentCount) {
            throw new SegmentNotFoundException(id);
        }
        return super.readSegment(id);
    }
}
