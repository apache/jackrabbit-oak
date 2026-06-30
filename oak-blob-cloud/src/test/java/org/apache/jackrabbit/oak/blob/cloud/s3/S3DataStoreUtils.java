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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.spi.blob.data.DataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.collections.MapUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.StringUtils;

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
        return List.of(S3.getName());
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

    public static boolean isS3EmulatorConfigured() {
        Properties props = getS3Config();
        return Objects.equals(S3EmulatorSupport.ACCESS_KEY, props.getProperty(S3Constants.ACCESS_KEY))
                && Objects.equals(S3EmulatorSupport.SECRET_KEY, props.getProperty(S3Constants.SECRET_KEY))
                && "true".equals(props.getProperty(S3Constants.PATH_STYLE_ACCESS))
                && props.getProperty(S3Constants.S3_END_POINT, "").startsWith("http://127.0.0.1:");
    }

    /**
     *
     * @return true if SSE_C encryption is configured
     */
    public static boolean isSseCustomerKeyEncrypted() {
        final Properties s3Config = getS3Config();
        return Objects.equals(Utils.getDataEncryption(s3Config), DataEncryption.SSE_C);
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
        if (StringUtils.isEmpty(config)) {
            File cfgFile = new File(System.getProperty("user.home"), DEFAULT_PROPERTY_FILE);
            if (cfgFile.exists()) {
                config = cfgFile.getAbsolutePath();
            }
        }
        if (StringUtils.isEmpty(config)) {
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
            Map<String, String> filtered = MapUtils.filterEntries(MapUtils.fromProperties(props),
                    input ->!StringUtils.isEmpty(input.getValue()));
            props = new Properties();
            props.putAll(filtered);
        }
        // Fall back to emulator when no real credentials are present.
        if (!hasRealCredentials(props) && S3EmulatorSupport.isAvailable()) {
            return S3EmulatorSupport.getEmulatorProperties();
        }
        return props;
    }

    private static boolean hasRealCredentials(Properties props) {
        String accessKey = props.getProperty(S3Constants.ACCESS_KEY, "").trim();
        String secretKey = props.getProperty(S3Constants.SECRET_KEY, "").trim();
        String region = props.getProperty(S3Constants.S3_REGION, "").trim();
        String endpoint = props.getProperty(S3Constants.S3_END_POINT, "").trim();
        return !accessKey.isEmpty() && !secretKey.isEmpty() && (!region.isEmpty() || !endpoint.isEmpty());
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

    public static void deleteBucket(String bucket, Date date) {
        log.info("cleaning bucket [ {} ]", bucket);
        Properties props = getS3Config();
        S3BackendHelper.deleteBucketAndAbortMultipartUploads(bucket, date, props);
    }

    protected static HttpURLConnection getHttpConnection(long length, URI uri) throws IOException {
        URLConnection raw = uri.toURL().openConnection();
        if (!(raw instanceof HttpURLConnection)) {
            throw new IOException("Expected HTTP(S) connection for URI: " + uri);
        }
        HttpURLConnection conn = (HttpURLConnection) raw;
        try {
            conn.setDoOutput(true);
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("Content-Length", String.valueOf(length));

            final Properties props = getS3Config();
            DataEncryption encryption = Utils.getDataEncryption(props);

            switch (encryption) {
                case SSE_S3:
                    conn.setRequestProperty("x-amz-server-side-encryption", "AES256");
                    System.out.println("Added SSE-S3 header: AES256");
                    break;
                case SSE_KMS:
                    conn.setRequestProperty("x-amz-server-side-encryption", "aws:kms");
                    System.out.println("Added SSE-KMS header: aws:kms");
                    if (Objects.nonNull(props.getProperty(S3Constants.S3_SSE_KMS_KEYID))) {
                        conn.setRequestProperty("x-amz-server-side-encryption-aws-kms-key-id", props.getProperty(S3Constants.S3_SSE_KMS_KEYID));
                        System.out.println("Added KMS Key ID: " + props.getProperty(S3Constants.S3_SSE_KMS_KEYID));
                    }
                    break;
                case SSE_C:
                    String customerKey = props.getProperty(S3Constants.S3_SSE_C_KEY);
                    String customerKeyMD5 = Utils.calculateMD5(customerKey);
                    conn.setRequestProperty("x-amz-server-side-encryption-customer-algorithm", "AES256");
                    conn.setRequestProperty("x-amz-server-side-encryption-customer-key", customerKey);
                    conn.setRequestProperty("x-amz-server-side-encryption-customer-key-MD5", customerKeyMD5);
                    break;
                case NONE:
                    break;
            }
        } catch (RuntimeException e) {
            conn.disconnect();
            throw e;
        }
        return conn;
    }

    protected static void doHttpUpload(InputStream in, long contentLength, URI uri) throws IOException {
        HttpURLConnection conn = getHttpConnection(contentLength, uri);
        try {
            try (OutputStream out = conn.getOutputStream()) {
                IOUtils.copy(in, out);
            }
            int responseCode = conn.getResponseCode();
            if (responseCode >= 400) {
                String errorBody = getErrorBody(conn);
                org.junit.Assert.fail(conn.getResponseMessage()
                        + (StringUtils.isEmpty(errorBody) ? "" : ": " + errorBody));
            }
        } finally {
            conn.disconnect();
        }
    }

    private static String getErrorBody(HttpURLConnection conn) {
        InputStream err = conn.getErrorStream();
        if (err == null) {
            return "";
        }
        try {
            return IOUtils.toString(err, StandardCharsets.UTF_8);
        } catch (IOException ignored) {
            return "";
        } finally {
            IOUtils.closeQuietly(err);
        }
    }
}
