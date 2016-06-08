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

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.Backend;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.S3Backend;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.S3DataStore;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.Utils;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class DataStoreUtils {
    private static final Logger log = LoggerFactory.getLogger(DataStoreUtils.class);

    public static final String DS_CLASS_NAME = "dataStore";

    private static final String DS_PROP_PREFIX = "ds.";
    private static final String BS_PROP_PREFIX = "bs.";

    /**
     * By default create a default directory. But if overridden will need to be unset
     */
    public static long time = -1;

    public static DataStoreBlobStore getBlobStore(File homeDir) throws Exception {
        return getBlobStore(homeDir.getAbsolutePath());
    }

    public static DataStoreBlobStore getBlobStore(String homeDir) throws Exception {
        String className = System.getProperty(DS_CLASS_NAME, OakFileDataStore.class.getName());
        DataStore ds = Class.forName(className).asSubclass(DataStore.class).newInstance();
        PropertiesUtil.populate(ds, getConfig(), false);
        ds.init(homeDir);
        return new DataStoreBlobStore(ds);
    }

    public static DataStoreBlobStore getBlobStore() throws Exception {
        return getBlobStore(getHomeDir());
    }

    public static boolean isS3DataStore() {
        String dsName = System.getProperty(DS_CLASS_NAME);
        return (dsName != null) && (dsName.equals(S3DataStore.class.getName()) || dsName
            .equals(SharedS3DataStore.class.getName()));
    }

    private static Map<String, ?> getConfig() {
        Map<String, Object> result = Maps.newHashMap();
        for (Map.Entry<String, ?> e : Maps.fromProperties(System.getProperties()).entrySet()) {
            String key = e.getKey();
            if (key.startsWith(DS_PROP_PREFIX) || key.startsWith(BS_PROP_PREFIX)) {
                key = key.substring(3); //length of bs.
                result.put(key, e.getValue());
            }
        }
        return result;
    }

    public static String getHomeDir() {
        return concat(new File(".").getAbsolutePath(), "target/blobstore/" +
            (time == -1 ? 0 : time));
    }

    public static FileDataStore createFDS(File root, int minRecordLength) {
        OakFileDataStore fds = new OakFileDataStore();
        fds.setPath(root.getAbsolutePath());
        fds.setMinRecordLength(minRecordLength);
        fds.init(null);
        return fds;
    }

    @Test
    public void testPropertySetup() throws Exception {
        System.setProperty(DS_CLASS_NAME, FileDataStore.class.getName());
        System.setProperty("ds.minRecordLength", "1000");

        DataStoreBlobStore dbs = getBlobStore();
        assertEquals(1000, dbs.getDataStore().getMinRecordLength());
    }

    /**
     * S3 specific cleanup
     *
     * @param dataStore the underlying DataStore instance
     * @param date the date of initialization
     * @throws Exception
     */
    public static void cleanup(DataStore dataStore, Date date) throws Exception {
        if (dataStore instanceof S3DataStore) {
            Backend backend = ((S3DataStore) dataStore).getBackend();
            if (backend instanceof S3Backend) {
                String bucket = ((S3Backend) backend).getBucket();
                deleteBucket(bucket, date);
            }
        }
    }

    private static void deleteBucket(String bucket, Date date) throws Exception {
        log.info("cleaning bucket [" + bucket + "]");
        Properties props = Utils.readConfig((String) getConfig().get("config"));
        AmazonS3Client s3service = Utils.openService(props);
        TransferManager tmx = new TransferManager(s3service);
        if (s3service.doesBucketExist(bucket)) {
            for (int i = 0; i < 4; i++) {
                tmx.abortMultipartUploads(bucket, date);
                ObjectListing prevObjectListing = s3service.listObjects(bucket);
                while (prevObjectListing != null ) {
                    List<DeleteObjectsRequest.KeyVersion>
                        deleteList = new ArrayList<DeleteObjectsRequest.KeyVersion>();
                    for (S3ObjectSummary s3ObjSumm : prevObjectListing.getObjectSummaries()) {
                        deleteList.add(new DeleteObjectsRequest.KeyVersion(
                            s3ObjSumm.getKey()));
                    }
                    if (deleteList.size() > 0) {
                        DeleteObjectsRequest delObjsReq = new DeleteObjectsRequest(
                            bucket);
                        delObjsReq.setKeys(deleteList);
                        s3service.deleteObjects(delObjsReq);
                    }
                    if (!prevObjectListing.isTruncated()) break;
                    prevObjectListing = s3service.listNextBatchOfObjects(prevObjectListing);
                }
            }
            //s3service.deleteBucket(bucket);
            log.info("bucket [ " + bucket + "] cleaned");
        } else {
            log.info("bucket [" + bucket + "] doesn't exists");
        }
        tmx.shutdownNow();
        s3service.shutdown();
    }
}
