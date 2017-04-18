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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Map;
import java.util.Properties;

import javax.jcr.RepositoryException;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CachingFileDataStore}.
 */
public class CachingFileDataStoreTest extends AbstractDataStoreTest {
    protected static final Logger LOG = LoggerFactory.getLogger(CachingFileDataStoreTest.class);

    private Properties props;
    private String fsBackendPath;

    @Override
    @Before
    public void setUp() throws Exception {
        fsBackendPath = folder.newFolder().getAbsolutePath();
        props = new Properties();
        props.setProperty("cacheSize", "0");
        props.setProperty("fsBackendPath", fsBackendPath);
        super.setUp();
    }

    protected DataStore createDataStore() throws RepositoryException {
        CachingFileDataStore ds = null;
        try {
            ds = new CachingFileDataStore();
            Map<String, ?> config = DataStoreUtils.getConfig();
            props.putAll(config);
            PropertiesUtil.populate(ds, Maps.fromProperties(props), false);
            ds.setProperties(props);
            ds.init(dataStoreDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ds;
    }

    @Test
    public void assertReferenceKey() throws Exception {
        byte[] data = new byte[dataLength];
        randomGen.nextBytes(data);
        DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
        Assert.assertEquals(data.length, rec.getLength());
        assertRecord(data, rec);
        DataRecord refRec = ds.getRecordFromReference(rec.getReference());
        assertRecord(data, refRec);

        // Check bytes retrieved from reference.key file
        File refFile = new File(fsBackendPath, "reference.key");
        assertTrue(refFile.exists());
        byte[] keyRet = FileUtils.readFileToByteArray(refFile);
        assertTrue(keyRet.length != 0);
    }

    @Override
    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * ---------- skip -----------
     **/
    @Override
    public void testUpdateLastModifiedOnAccess() {
    }

    @Override
    public void testDeleteAllOlderThan() {
    }
}
