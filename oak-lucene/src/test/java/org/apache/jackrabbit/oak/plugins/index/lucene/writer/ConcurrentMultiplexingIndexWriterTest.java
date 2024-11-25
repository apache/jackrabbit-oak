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
package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.newDoc;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DefaultDirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConcurrentMultiplexingIndexWriterTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = EMPTY_NODE.builder();
    private LuceneIndexDefinition defn = new LuceneIndexDefinition(root, builder.getNodeState(), "/foo");
    private MountInfoProvider mip = Mounts.newBuilder()
            .mount("foo", "/libs", "/apps")
            .readOnlyMount("ro", "/ro-tree")
            .build();
    private LuceneIndexWriterConfig writerConfig = new LuceneIndexWriterConfig();

    @Test
    public void concurrentWrite() throws Exception{
        LuceneIndexWriterFactory factory = newDirectoryFactory();
        final LuceneIndexWriter writer = factory.newInstance(defn, builder, null, true);
        
        int THREAD_COUNT = 100;
        final int LOOP_COUNT = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
        Exception[] firstException = new Exception[1];
        
        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    // wait for the signal
                    startLatch.await(); 
                    for (int j = 0; j < LOOP_COUNT; j++) {
                        writer.updateDocument("/libs/config", newDoc("/libs/config"));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    firstException[0] = e;
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        // signal
        startLatch.countDown();
        doneLatch.await();
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
        if (firstException[0] != null) {
            throw firstException[0];
        }
        writer.close(0);
    }    

    private LuceneIndexWriterFactory newDirectoryFactory(){
        return newDirectoryFactory(mip);
    }

    private LuceneIndexWriterFactory newDirectoryFactory(MountInfoProvider mountInfoProvider){
        DirectoryFactory directoryFactory = new DefaultDirectoryFactory(null, null);
        return new DefaultIndexWriterFactory(mountInfoProvider, directoryFactory, writerConfig);
    }

}
