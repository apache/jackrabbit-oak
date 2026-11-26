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
package org.apache.jackrabbit.oak.segment.azure.tool;

import com.azure.storage.blob.BlobContainerClient;
import org.apache.jackrabbit.oak.segment.azure.tool.AzureCheck.PersistentCacheType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AzureCheckCacheTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void defaultsToLegacyCachePath() throws Exception {
        Path path = folder.getRoot().toPath().resolve("cache");
        assertEquals(1, failingCheck(path).build().run());
        assertTrue(Files.isDirectory(path));
        assertFalse(Files.exists(path.resolve("00")));
    }

    @Test
    public void v2ModesUseConfiguredDirectoryAndCloseOnStartupFailure() throws Exception {
        Set<Thread> originalWriters = writerThreads();

        for (PersistentCacheType type : new PersistentCacheType[] {PersistentCacheType.SYNC, PersistentCacheType.ASYNC}) {
            Path path = folder.getRoot().toPath().resolve(type.name());
            StringWriter errors = new StringWriter();
            assertEquals(1, failingCheck(path).withErrWriter(new PrintWriter(errors))
                    .withPersistentCacheType(type).build().run());
            assertTrue(errors.toString(), errors.toString().contains("Test store startup failure"));
            assertTrue(Files.isDirectory(path.resolve("00")));
        }
        Set<Thread> remainingWriters = writerThreads();
        remainingWriters.removeAll(originalWriters);
        assertTrue("Azure check left cache writers running: " + remainingWriters, remainingWriters.isEmpty());
    }

    private static AzureCheck.Builder failingCheck(Path cachePath) {
        BlobContainerClient client = mock(BlobContainerClient.class);
        when(client.getBlobClient(anyString())).thenThrow(new IllegalStateException("Test store startup failure"));
        when(client.listBlobsByHierarchy(anyString())).thenThrow(new IllegalStateException("Test store startup failure"));
        return AzureCheck.builder()
                .withBlobContainerClient(client)
                .withRootPrefix("store")
                .withOutWriter(new PrintWriter(new StringWriter()))
                .withErrWriter(new PrintWriter(new StringWriter()))
                .withPersistentCachePath(cachePath.toString())
                .withPersistentCacheSizeGb(1);
    }

    private static Set<Thread> writerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith("disk-cache-writer-"))
                .collect(Collectors.toSet());
    }
}
