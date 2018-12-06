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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CopyOnReadDirectoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void multipleCloseCalls() throws Exception{
        final AtomicInteger executionCount = new AtomicInteger();
        Executor e = new Executor() {
            @Override
            public void execute(Runnable r) {
                executionCount.incrementAndGet();
                r.run();
            }
        };
        IndexCopier c = new IndexCopier(e, temporaryFolder.newFolder(), true);
        IndexDefinition def = mock(IndexDefinition.class);
        when(def.getReindexCount()).thenReturn(2L);

        Directory dir = c.wrapForRead("/oak:index/foo", def, new RAMDirectory());

        dir.close();
        dir.close();
        assertEquals(1, executionCount.get());
    }

}