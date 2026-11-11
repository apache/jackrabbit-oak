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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.jupiter.api.Test;

public class OakDirectoryTempFileNamingTest {

    @Test
    public void tempFilesUseLuceneTempFileNamesAndSkipExistingOnes() throws IOException {
        OakDirectory directory = new OakDirectory(EMPTY_NODE.builder(), "test-index", false);
        directory.createOutput("_0_bkd_1.tmp", IOContext.DEFAULT).close();

        try (IndexOutput first = directory.createTempOutput("_0", "bkd", IOContext.DEFAULT);
             IndexOutput second = directory.createTempOutput("_0", "bkd", IOContext.DEFAULT)) {
            assertEquals("_0_bkd_0.tmp", first.getName());
            assertEquals("_0_bkd_2.tmp", second.getName());
        }
        directory.close();
    }
}
