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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.Uuid;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class AzureStore implements Store {

    private final Properties config;
    private final CloudStorageAccount cloud;
    private final CloudBlobClient cloudBlobClient;
    private final CloudBlobContainer container;
    private final CloudBlobDirectory dir;
    private Compression compression = Compression.NO;
    private long writeCount;
    private long readCount;

    public String toString() {
        return "azure";
    }

    public AzureStore(Properties config) {
        this.config = config;
        try {
            cloud = CloudStorageAccount.parse(
                    config.getProperty("storageAccount"));
            cloudBlobClient = cloud.createCloudBlobClient();
            String maximumExecutionTimeInMs = config.getProperty("maximumExecutionTimeInMs");
            if (maximumExecutionTimeInMs != null) {
                cloudBlobClient.getDefaultRequestOptions().
                    setMaximumExecutionTimeInMs(Integer.parseInt(maximumExecutionTimeInMs));
            }
            container = cloudBlobClient.getContainerReference(
                    config.getProperty("container"));
            container.createIfNotExists();
            dir = container.getDirectoryReference(
                    config.getProperty("directory"));
        } catch (Exception e) {
; // TODO proper logging
e.printStackTrace();
            throw new IllegalArgumentException(config.toString(), e);
        }
    }

    @Override
    public void setWriteCompression(Compression compression) {
        this.compression = compression;
    }

    @Override
    public PageFile getIfExists(String key) {
        try {
            readCount++;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CloudAppendBlob blob = dir.getAppendBlobReference(key);
            blob.download(out);
            byte[] data = out.toByteArray();
            Compression c = Compression.getCompressionFromData(data[0]);
            data = c.expand(data);
            return PageFile.fromBytes(data);
        } catch (URISyntaxException | StorageException e) {
; // TODO proper logging
e.printStackTrace();
            throw new IllegalArgumentException(key, e);
        }
    }

    @Override
    public boolean supportsByteOperations() {
        return true;
    }

    @Override
    public byte[] getBytes(String key) {
        try {
            readCount++;
            CloudBlockBlob blob = dir.getBlockBlobReference(key);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            blob.download(out);
            return out.toByteArray();
        } catch (URISyntaxException | StorageException e) {
; // TODO proper logging
e.printStackTrace();
            throw new IllegalArgumentException(key, e);
        }
    }

    @Override
    public void putBytes(String key, byte[] data) {
        try {
            writeCount++;
            CloudBlockBlob blob = dir.getBlockBlobReference(key);
            blob.upload(new ByteArrayInputStream(data), data.length);
        } catch (URISyntaxException | StorageException | IOException e) {
; // TODO proper logging
e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void put(String key, PageFile value) {
        byte[] data = value.toBytes();
        data = compression.compress(data);
        CloudBlockBlob blob;
        try {
            writeCount++;
            blob = dir.getBlockBlobReference(key);
long start = System.nanoTime();
            blob.uploadFromByteArray(data, 0, data.length);
long time = System.nanoTime() - start;
System.out.println("Azure upload " + key + " size " + data.length + " nanos " + time);

        } catch (URISyntaxException | StorageException | IOException e) {
; // TODO proper logging
e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public String newFileName() {
        return Uuid.timeBasedVersion7().toShortString();
    }

    @Override
    public Set<String> keySet() {
        try {
            HashSet<String> set = new HashSet<>();
            for (ListBlobItem item : dir.listBlobs()) {
                if (item instanceof CloudBlockBlob) {
                    String name = ((CloudBlockBlob) item).getName();
                    int index = name.lastIndexOf('/');
                    if (index >= 0) {
                        name = name.substring(index + 1);
                    }
                    set.add(name);
                }
            }
            return set;
        } catch (StorageException | URISyntaxException e) {
; // TODO proper logging
e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void remove(Set<String> set) {
        try {
            for(String key : set) {
                CloudBlockBlob blob = dir.getBlockBlobReference(key);
                writeCount++;
                blob.deleteIfExists();
            }
        } catch (StorageException | URISyntaxException e) {
; // TODO proper logging
e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void removeAll() {
        remove(keySet());
    }

    @Override
    public long getWriteCount() {
        return writeCount;
    }

    @Override
    public long getReadCount() {
        return readCount;
    }

    @Override
    public void close() {
    }

    @Override
    public Properties getConfig() {
        return config;
    }

}
