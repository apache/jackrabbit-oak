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
package org.apache.jackrabbit.oak.run;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DownloaderTest {

    @Rule
    public TemporaryFolder sourceFolder = new TemporaryFolder();

    @Rule
    public TemporaryFolder destinationFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        FileUtils.cleanDirectory(sourceFolder.getRoot());
        FileUtils.cleanDirectory(destinationFolder.getRoot());
        // create sparse files
        try (RandomAccessFile file1 = new RandomAccessFile(sourceFolder.newFile("file1.txt"), "rw");
                RandomAccessFile file2 = new RandomAccessFile(sourceFolder.newFile("file2.txt"), "rw")) {
            file1.setLength(1024);
            file2.setLength(1024 * 1024);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void downloadInvalidConcurrency() throws IOException {
        try (Downloader downloader = new Downloader(0)) {
            downloader.download(Collections.emptyList());
        }
    }

    @Test
    public void downloadSingle() throws IOException {
        try (Downloader downloader = new Downloader(4)) {
            List<Downloader.ItemResponse> responses = downloader
                    .download(Collections.singletonList(createItem("file1.txt", "dest-file1.txt")));
            assertEquals(1, responses.size());
            assertFalse(responses.get(0).failed);
            assertEquals(1024, responses.get(0).size);

            File f = new File(destinationFolder.getRoot(), "dest-file1.txt");
            assertTrue(f.exists());
            assertTrue(f.isFile());
            assertEquals(1024, Files.size(f.toPath()));
        }
    }

    @Test
    public void downloadMulti() throws IOException {
        List<Downloader.Item> items = new ArrayList<>(2);
        items.add(createItem("file1.txt", "file1.txt"));
        items.add(createItem("file2.txt", "file2.txt"));
        try (Downloader downloader = new Downloader(4)) {
            List<Downloader.ItemResponse> responses = downloader.download(items);
            assertEquals(2, responses.size());
        }
    }

    @Test
    public void downloadMultiWithMissingOne() throws IOException {
        List<Downloader.Item> items = new ArrayList<>(2);
        items.add(createItem("file1.txt", "file1.txt"));
        items.add(createItem("file2.txt", "file2.txt"));
        items.add(createItem("file3.txt", "file3.txt"));
        try (Downloader downloader = new Downloader(4)) {
            List<Downloader.ItemResponse> responses = downloader.download(items);
            assertEquals(3, responses.size());

            Map<Boolean, List<Downloader.ItemResponse>> partitioned =
                    responses.stream().collect(Collectors.partitioningBy(ir -> ir.failed));

            List<Downloader.ItemResponse> success = partitioned.get(false);
            assertEquals(2, success.size());

            List<Downloader.ItemResponse> failures = partitioned.get(true);
            assertNotNull(failures.get(0).throwable);
        }
    }

    private Downloader.Item createItem(String source, String destination) throws MalformedURLException {
        Downloader.Item item = new Downloader.Item();
        item.source = new File(sourceFolder.getRoot(), source).toURI().toURL().toString();
        item.destination = new File(destinationFolder.getRoot(), destination).getAbsolutePath();
        return item;
    }

}
