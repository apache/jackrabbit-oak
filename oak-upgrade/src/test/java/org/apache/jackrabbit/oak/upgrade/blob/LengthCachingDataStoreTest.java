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

package org.apache.jackrabbit.oak.upgrade.blob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Random;

import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LengthCachingDataStoreTest {

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    @Test
    public void mappingFileData() throws Exception {
        File root = tempFolder.getRoot();
        File mappingFile = new File(root, "mapping.txt");
        String text = "1000|foo\n2000|bar";
        Files.write(text, mappingFile, Charset.defaultCharset());

        LengthCachingDataStore fds = new LengthCachingDataStore();
        fds.setDelegateClass(FileDataStore.class.getName());
        fds.setMappingFilePath(mappingFile.getAbsolutePath());
        fds.init(tempFolder.getRoot().getAbsolutePath());

        DataRecord dr = fds.getRecord(new DataIdentifier("foo"));
        assertNotNull(dr);
        assertEquals(1000, dr.getLength());

        assertEquals(2000, fds.getRecord(new DataIdentifier("bar")).getLength());
    }

    @Test
    public void configDelegate() throws Exception{
        //1. Store the config in a file
        Properties p = new Properties();
        p.setProperty("minRecordLength", "4972");
        File configFile = tempFolder.newFile();
        FileOutputStream fos = new FileOutputStream(configFile);
        p.store(fos, null);
        fos.close();

        //2. Configure the delegate and config file
        LengthCachingDataStore fds = new LengthCachingDataStore();
        fds.setDelegateClass(FileDataStore.class.getName());
        fds.setDelegateConfigFilePath(configFile.getAbsolutePath());
        fds.init(tempFolder.getRoot().getAbsolutePath());

        assertEquals(4972, fds.getMinRecordLength());
    }

    @Test
    public void delegateRecordTest() throws Exception{
        FileDataStore ds = new FileDataStore();
        byte[] data = bytes(ds.getMinRecordLength() + 10);
        ds.init(tempFolder.getRoot().getAbsolutePath());
        DataRecord dr = ds.addRecord(new ByteArrayInputStream(data));

        File mappingFile = new File(tempFolder.getRoot(), "mapping.txt");
        String text = String.format("%s|%s", data.length, dr.getIdentifier().toString());
        Files.write(text, mappingFile, Charset.defaultCharset());

        LengthCachingDataStore fds = new LengthCachingDataStore();
        fds.setDelegateClass(FileDataStore.class.getName());
        fds.setMappingFilePath(mappingFile.getAbsolutePath());
        fds.init(tempFolder.getRoot().getAbsolutePath());

        DataRecord dr2 = fds.getRecordIfStored(dr.getIdentifier());
        assertEquals(dr, dr2);

        assertEquals(dr.getLength(), dr2.getLength());
        assertTrue(supplier(dr).contentEquals(supplier(dr2)));
    }

    @Test
    public void writeBackNewEntries() throws Exception{
        //1. Add some entries to FDS
        FileDataStore fds1 = new FileDataStore();
        File fds1Dir = tempFolder.newFolder();
        int minSize = fds1.getMinRecordLength();
        fds1.init(fds1Dir.getAbsolutePath());
        DataRecord dr1 = fds1.addRecord(byteStream(minSize + 10));
        DataRecord dr2 = fds1.addRecord(byteStream(minSize + 100));

        //2. Try reading them so as to populate the new mappings
        LengthCachingDataStore fds2 = new LengthCachingDataStore();
        fds2.setDelegateClass(FileDataStore.class.getName());
        fds2.init(fds1Dir.getAbsolutePath());

        fds2.getRecord(new DataIdentifier(dr1.getIdentifier().toString()));
        fds2.getRecord(new DataIdentifier(dr2.getIdentifier().toString()));

        File mappingFile = fds2.getMappingFile();

        //3. Get the mappings pushed to file
        fds2.close();

        //4. Open a new FDS pointing to new directory. Read should still work fine
        //as they would be served by the mapping data
        LengthCachingDataStore fds3 = new LengthCachingDataStore();
        fds3.setDelegateClass(FileDataStore.class.getName());
        fds3.setMappingFilePath(mappingFile.getAbsolutePath());
        fds3.init(tempFolder.newFolder().getAbsolutePath());
        fds3.setReadOnly(false);

        assertEquals(dr1.getLength(), fds3.getRecord(dr1.getIdentifier()).getLength());
        assertEquals(dr2.getLength(), fds3.getRecord(dr2.getIdentifier()).getLength());

        DataRecord dr3 = fds3.addRecord(byteStream(minSize + 200));
        //5. Close again so see if update of existing file works
        fds3.close();

        LengthCachingDataStore fds4 = new LengthCachingDataStore();
        fds4.setDelegateClass(FileDataStore.class.getName());
        fds4.setMappingFilePath(mappingFile.getAbsolutePath());
        fds4.init(tempFolder.newFolder().getAbsolutePath());

        assertEquals(dr3.getLength(), fds4.getRecord(dr3.getIdentifier()).getLength());
        assertEquals(dr2.getLength(), fds4.getRecord(dr2.getIdentifier()).getLength());


    }

    @Test
    public void referenceHandling() throws Exception{
        int minSize =  new FileDataStore().getMinRecordLength();
        LengthCachingDataStore fds = new LengthCachingDataStore();
        fds.setDelegateClass(FileDataStore.class.getName());
        fds.init(tempFolder.newFolder().getAbsolutePath());
        fds.setReadOnly(false);

        DataRecord dr1 = fds.addRecord(byteStream(minSize + 10));
        assertNotNull(fds.getRecordFromReference(dr1.getReference()));
        assertEquals(dr1.getIdentifier(), fds.getRecordFromReference(dr1.getReference()).getIdentifier());
    }

    private InputStream byteStream(int size) {
        return new ByteArrayInputStream(bytes(size));
    }

    private byte[] bytes(int size) {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return data;
    }

    private static ByteSource supplier(final DataRecord dr) {
        return new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
                try {
                    return dr.getStream();
                } catch (DataStoreException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
