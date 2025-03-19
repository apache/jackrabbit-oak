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
package org.apache.jackrabbit.oak.segment.azure.journal.v8;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.azure.v8.ReverseFileReaderV8;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ReverseFileReaderV8Test {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
        getBlob().createOrReplace();
    }

    private CloudAppendBlob getBlob() throws URISyntaxException, StorageException {
        return container.getAppendBlobReference("test-blob");
    }

    @Test
    public void testReverseReader() throws IOException, URISyntaxException, StorageException {
        List<String> entries = createFile( 1024, 80);
        ReverseFileReaderV8 reader = new ReverseFileReaderV8(getBlob(), 256);
        assertEquals(entries, reader);
    }

    @Test
    public void testEmptyFile() throws IOException, URISyntaxException, StorageException {
        List<String> entries = createFile( 0, 80);
        ReverseFileReaderV8 reader = new ReverseFileReaderV8(getBlob(), 256);
        assertEquals(entries, reader);
    }

    @Test
    public void test1ByteBlock() throws IOException, URISyntaxException, StorageException {
        List<String> entries = createFile( 10, 16);
        ReverseFileReaderV8 reader = new ReverseFileReaderV8(getBlob(), 1);
        assertEquals(entries, reader);
    }


    private List<String> createFile(int lines, int maxLineLength) throws IOException, URISyntaxException, StorageException {
        Random random = new Random();
        List<String> entries = new ArrayList<>();
        CloudAppendBlob blob = getBlob();
        for (int i = 0; i < lines; i++) {
            int entrySize = random.nextInt(maxLineLength) + 1;
            String entry = randomString(entrySize);
            try {
                blob.appendText(entry + '\n');
            } catch (StorageException e) {
                throw new IOException(e);
            }
            entries.add(entry);
        }

        entries.add("");
        Collections.reverse(entries);
        return entries;
    }

    private static void assertEquals(List<String> entries, ReverseFileReaderV8 reader) throws IOException {
        int i = entries.size();
        for (String e : entries) {
            Assert.assertEquals("line " + (--i), e, reader.readLine());
        }
        Assert.assertNull(reader.readLine());
    }

    private static String randomString(int entrySize) {
        Random r = new Random();

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < entrySize; i++) {
            result.append((char) ('a' + r.nextInt('z' - 'a')));
        }

        return result.toString();
    }
}
