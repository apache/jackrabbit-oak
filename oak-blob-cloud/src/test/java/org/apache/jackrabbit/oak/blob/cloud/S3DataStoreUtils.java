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
package org.apache.jackrabbit.oak.blob.cloud;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.jackrabbit.core.data.Backend;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.S3Backend;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.S3DataStore;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.SharedS3DataStore;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.Utils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension to {@link DataStoreUtils} to enable S3 extensions for cleaning.
 */
public class S3DataStoreUtils extends DataStoreUtils {
    private static final Logger log = LoggerFactory.getLogger(S3DataStoreUtils.class);

    public static boolean isS3DataStore() {
        String dsName = System.getProperty(DS_CLASS_NAME);
        return (dsName != null) && (dsName.equals(S3DataStore.class.getName()) || dsName
            .equals(SharedS3DataStore.class.getName()));
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
