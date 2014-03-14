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
package org.apache.jackrabbit.oak.plugins.document.blob.ds;

import java.io.File;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.junit.Test;

import static org.apache.commons.io.FilenameUtils.concat;
import static org.junit.Assert.assertEquals;

/**
 * Helper for retrieving the {@link DataStoreBlobStore} instantiated via system properties
 *
 * User must specify the class of DataStore to use via 'dataStore' system property
 *
 * Further to configure properties of DataStore instance one can specify extra system property
 * where the key has a prefix 'ds.' or 'bs.'. So to set 'minRecordLength' of FileDataStore specify
 * the system property as 'ds.minRecordLength'
 */
public class DataStoreUtils extends AbstractMongoConnectionTest {
    public static final String DS_CLASS_NAME = "dataStore";
    public static final String PATH = "./target/repository/";

    private static final String DS_PROP_PREFIX = "ds.";
    private static final String BS_PROP_PREFIX = "bs.";

    public static DataStoreBlobStore getBlobStore() throws Exception {
        String className = System.getProperty(DS_CLASS_NAME);
        if(className != null){
            DataStore ds = Class.forName(className).asSubclass(DataStore.class).newInstance();
            ds.init(getHomeDir());
            PropertiesUtil.populate(ds, getConfig() , false);
            return new DataStoreBlobStore(ds);
        }
        return null;
    }

    private static Map<String,?> getConfig(){
        Map<String,Object> result = Maps.newHashMap();
        for(Map.Entry<String,?> e : Maps.fromProperties(System.getProperties()).entrySet()){
            String key = e.getKey();
            if(key.startsWith(DS_PROP_PREFIX) || key.startsWith(BS_PROP_PREFIX)){
                key = key.substring(3); //length of bs.
                result.put(key, e.getValue());
            }
        }
        return result;
    }

    private static String getHomeDir() {
        return concat( new File(".").getAbsolutePath(), "target/blobstore/"+System.currentTimeMillis());
    }

    @Test
    public void testPropertySetup() throws Exception {
        System.setProperty(DS_CLASS_NAME, FileDataStore.class.getName());
        System.setProperty("ds.minRecordLength", "1000");

        DataStoreBlobStore dbs = getBlobStore();
        assertEquals(1000, dbs.getDataStore().getMinRecordLength());
    }
}
