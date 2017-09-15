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

package org.apache.jackrabbit.oak.segment.standby.server;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DefaultStandbyHeadReaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore newFileStore() throws Exception {
        return fileStoreBuilder(folder.getRoot()).build();
    }

    @Test
    public void shouldReturnHeadSegmentId() throws Exception {
        try (FileStore store = newFileStore()) {
            store.flush();
            DefaultStandbyHeadReader reader = new DefaultStandbyHeadReader(store);
            assertEquals(store.getRevisions().getPersistedHead().toString(), reader.readHeadRecordId());
        }
    }

}
