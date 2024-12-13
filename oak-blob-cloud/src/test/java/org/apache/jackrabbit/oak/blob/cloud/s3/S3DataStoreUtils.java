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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Backend.RemoteStorageMode;

/**
 * Extension to {@link DataStoreUtils} to enable S3 extensions for cleaning and initialization.
 */
public class S3DataStoreUtils extends DataStoreUtils {
    private static final Logger log = LoggerFactory.getLogger(S3DataStoreUtils.class);

    static final String DEFAULT_CONFIG_PATH = "./src/test/resources/aws.properties";
    private static final String DEFAULT_PROPERTY_FILE = "aws.properties";
    private static final String SYS_PROP_NAME = "s3.config";

    protected static Class S3 = S3DataStore.class;

    public static List<String> getFixtures() {
        return ImmutableList.of(S3.getName());
    }

    public static boolean isS3DataStore() {
        String dsName = System.getProperty(DS_CLASS_NAME);
        boolean s3Class = (dsName != null) && (dsName.equals(S3.getName()));
        if (!isS3Configured()) {
            return false;
        }
        return s3Class;
    }

    /**
     * Check for presence of mandatory properties.
     *
     * @return true if mandatory props configured.
     */
    public static boolean isS3Configured() {
        Properties props = getS3Config();
        if (!props.containsKey(S3Constants.ACCESS_KEY) || !props.containsKey(S3Constants.SECRET_KEY) || !(
            props.containsKey(S3Constants.S3_REGION) || props.containsKey(S3Constants.S3_END_POINT))) {

            return false;
        }
        return true;
    }

    /**
     * Read any config property configured.
     * Also, read any props available as system properties.
     * System properties take precedence.
     *
     * @return Properties instance
     */
    public static Properties getS3Config() {
        String config = System.getProperty(SYS_PROP_NAME);
        if (Strings.isNullOrEmpty(config)) {
            File cfgFile = new File(System.getProperty("user.home"), DEFAULT_PROPERTY_FILE);
            if (cfgFile.exists()) {
                config = cfgFile.getAbsolutePath();
            }
        }
        if (Strings.isNullOrEmpty(config)) {
            config = DEFAULT_CONFIG_PATH;
        }
        Properties props = new Properties();
        if (new File(config).exists()) {
            InputStream is = null;
            try {
                is = new FileInputStream(config);
                props.load(is);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeQuietly(is);
            }
            props.putAll(getConfig());
            Map filtered = Maps.filterEntries(Maps.fromProperties(props),
                    input ->!Strings.isNullOrEmpty((String) input.getValue()));
            props = new Properties();
            props.putAll(filtered);
        }
        return props;
    }

    public static DataStore getS3DataStore(String className, Properties props, String homeDir) throws Exception {
        DataStore ds = Class.forName(className).asSubclass(DataStore.class).newInstance();
        PropertiesUtil.populate(ds, Utils.asMap(props), false);
        // Set the props object
        if (S3.getName().equals(className)) {
            ((S3DataStore) ds).setProperties(props);
        }
        ds.init(homeDir);

        return ds;
    }

    public static DataStore getS3DataStore(String className, String homeDir) throws Exception {
        return getS3DataStore(className, getS3Config(), homeDir);
    }

    public static void deleteBucket(String bucket, Date date) throws Exception {
        log.info("cleaning bucket [" + bucket + "]");
        Properties props = getS3Config();
        AmazonS3Client s3service = Utils.openService(props);
        TransferManager tmx = new TransferManager(s3service);
        if (s3service.doesBucketExist(bucket)) {
            for (int i = 0; i < 4; i++) {
                tmx.abortMultipartUploads(bucket, date);
                ObjectListing prevObjectListing = s3service.listObjects(bucket);
                while (prevObjectListing != null) {
                    List<DeleteObjectsRequest.KeyVersion> deleteList = new ArrayList<DeleteObjectsRequest.KeyVersion>();
                    List<String> keysToDelete = new ArrayList<>();
                    for (S3ObjectSummary s3ObjSumm : prevObjectListing.getObjectSummaries()) {
                        deleteList.add(new DeleteObjectsRequest.KeyVersion(s3ObjSumm.getKey()));
                        keysToDelete.add(s3ObjSumm.getKey());
                    }
                    if (!deleteList.isEmpty()) {
                        RemoteStorageMode mode = getMode(props);
                        if (mode == RemoteStorageMode.S3) {
                            DeleteObjectsRequest delObjsReq = new DeleteObjectsRequest(bucket);
                            delObjsReq.setKeys(deleteList);
                            s3service.deleteObjects(delObjsReq);
                        } else {
                            keysToDelete.forEach(key -> s3service.deleteObject(bucket, key));
                        }
                    }
                    if (!prevObjectListing.isTruncated())
                        break;
                    prevObjectListing = s3service.listNextBatchOfObjects(prevObjectListing);
                }
            }
            s3service.deleteBucket(bucket);
            log.info("bucket [ " + bucket + "] cleaned");
        } else {
            log.info("bucket [" + bucket + "] doesn't exists");
        }
        tmx.shutdownNow();
        s3service.shutdown();
    }

    @NotNull
    private static RemoteStorageMode getMode(@NotNull Properties props) {
        return props.getProperty(S3Constants.S3_END_POINT, "").contains("googleapis") ?
                RemoteStorageMode.GCP : RemoteStorageMode.S3;
    }

    protected static HttpsURLConnection getHttpsConnection(long length, URI uri) throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) uri.toURL().openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(length));
        conn.setRequestProperty("Date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX").withZone(ZoneOffset.UTC).format(Instant.now()));
        conn.setRequestProperty("Host", uri.getHost());

        return conn;
    }
}
