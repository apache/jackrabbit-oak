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

import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.commons.internal.concurrent.ExecutorUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;

public class CopyOnReadDirectoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void multipleCloseCalls() throws Exception {
        AtomicInteger executionCount = new AtomicInteger();
        Executor e = r -> {executionCount.incrementAndGet(); r.run();};
        LuceneNgIndexCopier c = new LuceneNgIndexCopier(ExecutorUtils.directExecutor(), temporaryFolder.newFolder(), true);

        // remote must be a real OakDirectory (this module's only remote implementation,
        // see LuceneNgIndexCopierTest's construction pattern) rather than a generic
        // in-memory Directory stand-in - CopyOnReadDirectory's constructor now requires it.
        NodeBuilder storageBuilder = INITIAL_CONTENT.builder();
        OakDirectory remote = new OakDirectory(storageBuilder, "testIndex", false);

        // Not opened via try-with-resources: CopyOnReadDirectory.close() (below) already
        // closes `local` as part of its own close path, so a second close here would
        // double-close it.
        File localDir = temporaryFolder.newFolder();
        Directory local = FSDirectory.open(localDir.toPath());
        Directory dir = new CopyOnReadDirectory(c, remote, local, localDir, false, "foo", e);

        dir.close();
        dir.close();
        assertEquals(1, executionCount.get());
    }
}
