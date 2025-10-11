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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.blob.cloud.s3.S3Backend.RemoteStorageMode;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListMultipartUploadsIterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.utils.BinaryUtils;
import software.amazon.awssdk.utils.Md5Utils;
import software.amazon.awssdk.utils.StringUtils;

import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION;

/**
 * Amazon S3 utilities.
 */
public final class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static final String DEFAULT_CONFIG_FILE = "aws.properties";

    private static final String DELETE_CONFIG_SUFFIX = ";burn";

    private static final String HTTPS = "https";

    /**
     * The default value AWS bucket region.
     */
    public static final String DEFAULT_AWS_BUCKET_REGION = "us-standard";

    /**
     * The value for the us-east-1 region.
     */
    public static final String US_EAST_1_AWS_BUCKET_REGION = "us-east-1";

    /**
     * constants to define endpoint to various AWS region
     */
    public static final String AWSDOTCOM = "amazonaws.com";

    public static final String S3 = "s3";
    public static final String S3_ACCELERATION = "s3-accelerate";

    public static final String DOT = ".";

    public static final String DASH = "-";

    /**
     * private constructor so that class cannot initialize from outside.
     */
    private Utils() {

    }

    /**
     * Create S3Client from properties.
     *
     * @param prop properties to configure @link {@link S3Client}
     * @param accReq boolean indicating whether to accelerate requests
     * @return {@link S3Client}
     */
    public static S3Client openService(final Properties prop, boolean accReq) {

        S3ClientBuilder builder = S3Client.builder();

        configureBuilder(builder, prop, accReq);
        // sync http client
        builder.httpClient(getSdkHttpClient(prop));

        return builder.build();
    }

    /**
     * Create S3Client from properties.
     *
     * @param prop properties to configure @link {@link S3Client}
     * @return {@link S3Client}
     */
    public static S3AsyncClient openAsyncService(final Properties prop) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder();

        configureBuilder(builder, prop, false);
        // async http client
        builder.httpClient(getSdkAsyncHttpClient(prop));
        builder.multipartEnabled(true);

        return builder.build();
    }

    /**
     * Creates an {@link S3Presigner} instance using the provided {@link S3Client} and S3 configuration properties.
     * <p>
     * The presigner is used to generate pre-signed URLs for S3 operations, allowing secure, time-limited access
     * to S3 resources without exposing credentials.
     * </p>
     *
     * @param s3Client the {@link S3Client} to use for presigning requests
     * @param props the properties containing S3 configuration (credentials, region, etc.)
     * @return a configured {@link S3Presigner} instance
     */
    public static S3Presigner createPresigner(final S3Client s3Client, final Properties props) {
        return S3Presigner.builder().s3Client(s3Client).
                credentialsProvider(Utils.getAwsCredentials(props))
                .region(Region.of(Utils.getRegion(props)))
                .build();
    }

    /**
     * Waits for an S3 bucket, one we expect to exist, to report that it exists.
     * A check for the bucket is called with a limited number of repeats with
     * an increasing backoff.
     *
     * Usually you would call this after creating a bucket to block until the
     * bucket is actually available before moving forward with other tasks that
     * expect the bucket to be available.
     *
     * @param s3Client The AmazonS3 client connection to the storage service.
     * @param bucketName The name of the bucket to check.
     * @return True if the bucket exists; false otherwise.
     */
    public static boolean waitForBucket(@NotNull final S3Client s3Client, final String bucketName) {
        int tries = 0;
        boolean bucketExists = false;
        while (tries < 20) {
            tries++;
            try {
                s3Client.headBucket(headReq -> headReq.bucket(bucketName).build());
                bucketExists = true;
                break;
            } catch (NoSuchBucketException e) {
                // Bucket does not exist, wait and retry
                try {
                    Thread.sleep(100L * tries);
                } catch (InterruptedException ie) {}
            } catch (Exception e) {
                // For other exceptions, optionally handle or break
                break;
            }
        }
        return bucketExists;
    }

    /**
     * Delete S3 bucket. This method first deletes all objects from bucket and
     * then delete empty bucket.
     *
     * @param bucketName the bucket name.
     */
    public static void deleteBucket(final String bucketName) throws IOException {
        Properties prop = readConfig(DEFAULT_CONFIG_FILE);
        S3Client s3Client = openService(prop, false);

        deleteBucketObjects(bucketName, prop, s3Client);

        // Delete the actual bucket
        s3Client.deleteBucket(delReq -> delReq.bucket(bucketName).build());
    }

    public static void deleteBucketAndAbortMultipartUploads(final String bucket, final Date date, final Properties props) {
        try (S3Client s3service = Utils.openService(props, false)) {
            if (!Utils.bucketExists(s3service, bucket)) {
                LOG.info("bucket [{}] doesn't exists", bucket);
                return;
            }

            for (int i = 0; i < 4; i++) {
                abortMultipartUpload(bucket, date, s3service);
            }
            // Delete objects in the bucket with pagination
            deleteBucketObjects(bucket, props, s3service);
            // Delete the bucket
            s3service.deleteBucket(delBucket -> delBucket.bucket(bucket).build());
            LOG.info("bucket [ {} ] cleaned", bucket);
        }
    }

    public static void deleteBucketObjects(final String bucket, final Properties props, final S3Client s3service) {
        deleteBucketObjects(bucket, props, s3service, s3service.listObjectsV2Paginator(
                ListObjectsV2Request.builder().bucket(bucket).build()));
    }

    /**
     * Read a configuration properties file. If the file name ends with ";burn",
     * the file is deleted after reading.
     *
     * @param fileName the properties file name
     * @return the properties
     * @throws java.io.IOException if the file doesn't exist
     */
    public static Properties readConfig(String fileName) throws IOException {
        boolean delete = false;
        if (fileName.endsWith(DELETE_CONFIG_SUFFIX)) {
            delete = true;
            fileName = fileName.substring(0, fileName.length()
                    - DELETE_CONFIG_SUFFIX.length());
        }
        if (!new File(fileName).exists()) {
            throw new IOException("Config file not found: " + fileName);
        }
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(fileName);
            prop.load(in);
        } finally {
            if (in != null) {
                in.close();
            }
            if (delete) {
                deleteIfPossible(new File(fileName));
            }
        }
        return prop;
    }

    public static Map<String, Object> asMap(Properties props) {
        Map<String, Object> map = new HashMap<>();
        for (Object key : props.keySet()) {
            map.put((String)key, props.get(key));
        }
        return map;
    }

    /**
     * Determines the data encryption type to use for S3 operations based on the provided properties.
     * <p>
     * If the property {@code S3_ENCRYPTION} is set, returns the corresponding {@link DataEncryption} enum value.
     * If the property is not set or is invalid, returns {@link DataEncryption#NONE}.
     *
     * @param props the properties containing S3 configuration
     * @return the {@link DataEncryption} type to use
     */
    public static DataEncryption getDataEncryption(final Properties props) {
        final String encryptionType = props.getProperty(S3_ENCRYPTION);
        DataEncryption encryption = DataEncryption.NONE;
        if (encryptionType != null) {
            encryption = DataEncryption.valueOf(encryptionType);
        }
        return encryption;
    }

    /**
     * Sets the remote storage mode in the provided properties based on the S3 endpoint.
     * <p>
     * If the endpoint contains "googleapis", the mode is set to GCP. Otherwise, it is set to S3.
     * If the mode was previously set to S3 but the endpoint is for GCP, a warning is logged and the mode is overridden.
     *
     * @param properties the properties to update with the remote storage mode
     */
    public static void setRemoteStorageMode(final Properties properties) {
        String s3EndPoint = properties.getProperty(S3Constants.S3_END_POINT, "");
        if (s3EndPoint.contains("googleapis")) {
            if (properties.get(S3Constants.MODE) == RemoteStorageMode.S3) {
                LOG.warn("Mismatch between remote storage mode and s3EndPoint, overriding mode to GCP");
            }
            properties.put(S3Constants.MODE, RemoteStorageMode.GCP);
            return;
        }
        // default mode is S3
        properties.put(S3Constants.MODE, RemoteStorageMode.S3);
    }

    /**
     * Determines the AWS region to use based on the provided properties.
     * <p>
     * The method attempts to extract the region in the following order:
     * <ol>
     *   <li>If the S3 endpoint property is set, tries to parse the region from the endpoint URL.</li>
     *   <li>If not found, uses the S3 region property.</li>
     *   <li>If the region property is empty, falls back to the default region from the environment.</li>
     *   <li>If the region is "us-standard", returns "us-east-1".</li>
     * </ol>
     *
     * @param prop the properties containing S3 configuration
     * @return the AWS region as a string
     */
    static String getRegion(final Properties prop) {

        String region = null;

        if (Objects.nonNull(prop.getProperty(S3Constants.S3_END_POINT))) {
            region = getRegionFromEndpoint(prop.getProperty(S3Constants.S3_END_POINT));
        }

        if (Objects.nonNull(region)) {
            return region;
        }

        region = prop.getProperty(S3Constants.S3_REGION);

        if (region.isEmpty()) {
            region = Utils.getDefaultRegion();
        }

        if (Objects.equals(DEFAULT_AWS_BUCKET_REGION, region)) {
            return US_EAST_1_AWS_BUCKET_REGION;
        }
        return region;
    }

    /**
     * Extracts the AWS region from a given S3 endpoint URL.
     * <p>
     * Supports both path-style and virtual-hosted-style S3 endpoints, e.g.:
     * <ul>
     *   <li>https://s3.eu-west-1.amazonaws.com</li>
     *   <li>https://bucket.s3.eu-west-1.amazonaws.com</li>
     *   <li>https://s3.amazonaws.com (returns us-east-1)</li>
     * </ul>
     * If the region cannot be determined, returns null.
     *
     * @param endpoint the S3 endpoint URL as a string
     * @return the AWS region string, or null if not found
     */
    static String getRegionFromEndpoint(final String endpoint) {
        try {
            URI uri = URI.create(endpoint);
            String host = uri.getHost();

            // Pattern for standard S3 endpoints: s3.region.amazonaws.com or bucket.s3.region.amazonaws.com
            Pattern pattern = Pattern.compile("s3[.-]([a-z0-9-]+)\\.amazonaws\\.com");
            Matcher matcher = pattern.matcher(host);

            if (matcher.find()) {
                return matcher.group(1);
            }

            // Handle us-east-1 special case (s3.amazonaws.com)
            // Handle virtual-hosted-style URLs: bucket.s3.amazonaws.com
            if (host.equals("s3.amazonaws.com") || host.endsWith(".s3.amazonaws.com")) {
                return Region.US_EAST_1.id();
            }

            LOG.warn("Cannot parse region from endpoint: {}", endpoint);

            return null;

        } catch (Exception e) {
            LOG.error("Invalid endpoint format: {}", endpoint);
            return null;
        }
    }

    static void deleteBucketObjects(final String bucket, final Properties props, final S3Client s3service,
                                            final ListObjectsV2Iterable listResponses) {
        for (ListObjectsV2Response listRes : listResponses) {
            List<ObjectIdentifier> deleteList = new ArrayList<>();
            for (S3Object s3Obj : listRes.contents()) {
                deleteList.add(ObjectIdentifier.builder().key(s3Obj.key()).build());
            }

            if (!deleteList.isEmpty()) {
                RemoteStorageMode mode = getMode(props);
                if (mode == RemoteStorageMode.S3) {
                    s3service.deleteObjects(delReq ->
                            delReq.bucket(bucket)
                                    .delete(delObj ->
                                            delObj.objects(deleteList)
                                                    .build())
                                    .build());
                } else {
                    // Delete objects one by one
                    deleteList.forEach(obj -> s3service.deleteObject(delObj -> delObj
                            .bucket(bucket)
                            .key(obj.key())
                            .build()));

                }
            }
        }
    }

    static void abortMultipartUpload(String bucket, Date date, S3Client s3Client) {
        ListMultipartUploadsIterable multiUploadsResponse = s3Client.listMultipartUploadsPaginator(listReq ->
                listReq.bucket(bucket).build());
        for (MultipartUpload multipartUpload : multiUploadsResponse.uploads()) {
            Instant initiated = multipartUpload.initiated();
            if (initiated.isBefore(date.toInstant())) {
                s3Client.abortMultipartUpload(abortReq -> abortReq
                        .bucket(bucket)
                        .key(multipartUpload.key())
                        .uploadId(multipartUpload.uploadId())
                        .build());
            }
        }
    }

    /**
     * Checks if the specified Amazon S3 bucket exists and is accessible with the provided S3 client.
     * <p>
     * This method attempts to perform a {@code headBucket} request. If the bucket exists and is accessible,
     * it returns {@code true}. If the bucket does not exist, it returns {@code false}.
     * Other unexpected exceptions (such as permission errors or network issues) will propagate.
     * </p>
     *
     * @param s3Client   the {@link S3Client} to use for the request
     * @param bucketName the name of the S3 bucket to check
     * @return {@code true} if the bucket exists and is accessible; {@code false} if the bucket does not exist
     * @throws NullPointerException if {@code s3Client} or {@code bucketName} is null
     * @throws S3Exception         if an AWS error other than {@link NoSuchBucketException} occurs
     */
    static boolean bucketExists(final S3Client s3Client, final String bucketName) {
        try {
            s3Client.headBucket(request -> request.bucket(bucketName));
            return true;
        }
        catch (NoSuchBucketException exception) {
            return false;
        }
    }

    /**
     * Checks if a specific object exists in the given S3 bucket.
     * <p>
     * Performs a {@code headObject} request using the provided {@link S3RequestDecorator}.
     * Returns {@code true} if the object exists, {@code false} if it does not (404 or 403).
     * Other {@link S3Exception}s are propagated.
     * </p>
     *
     * @param s3Client        the {@link S3Client} to use for the request
     * @param bucket          the S3 bucket name
     * @param key             the object key
     * @param s3ReqDecorator  decorator for the {@link HeadObjectRequest}
     * @return {@code true} if the object exists, {@code false} if not found or forbidden
     * @throws S3Exception for AWS errors other than 404 or 403
     */
    static boolean objectExists(final S3Client s3Client, final String bucket, final String key, S3RequestDecorator s3ReqDecorator) {
        try {
            s3Client.headObject(s3ReqDecorator.decorate(HeadObjectRequest.builder().bucket(bucket).key(key).build()));

            LOG.debug("Object {} exists in bucket {}", key, bucket);
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404 || e.statusCode() == 403) {
                LOG.info("Object {} doesn't exists in bucket {}", key, bucket);
                return false;
            } else {
                throw e;
            }
        }
    }

    /**
     * Constructs the S3 endpoint URI based on the provided properties, acceleration flag, and region.
     * <p>
     * The method determines the endpoint as follows:
     * <ol>
     *   <li>If the S3 endpoint property is set, uses it directly.</li>
     *   <li>Otherwise, constructs the endpoint using the region and acceleration flag.</li>
     *   <li>The protocol is taken from the properties or defaults to "https".</li>
     * </ol>
     *
     * @param prop   the properties containing S3 configuration
     * @param accReq whether to use S3 acceleration endpoint
     * @param region the AWS region
     * @return the constructed S3 endpoint URI
     */
    @NotNull
    static URI getEndPointUri(Properties prop, boolean accReq, String region) {
        String endPoint;
        String propEndPoint = prop.getProperty(S3Constants.S3_END_POINT);
        if (propEndPoint != null && !propEndPoint.isEmpty()) {
            endPoint = propEndPoint;
        } else {
            // in aws sdk 2.x, there is no us-standard region. us-east-1 should be used in its place.
            endPoint = (accReq ? S3_ACCELERATION : S3) + DOT + region + DOT + AWSDOTCOM;

        }

        /*
         * setting endpoint to remove latency of redirection. If endpoint is
         * not set, invocation first goes us standard region, which
         * redirects it to correct location.
         */

        // Check if endpoint already contains protocol
        if (endPoint.startsWith("http://") || endPoint.startsWith("https://")) {
            LOG.info("S3 service endpoint [{}] ", endPoint);
            return URI.create(endPoint);
        }

        String protocol = prop.getProperty(S3Constants.S3_CONN_PROTOCOL);
        if (protocol == null || protocol.isEmpty()) {
            protocol = HTTPS; // default protocol
        }

        LOG.info("S3 service endpoint [{}] ", endPoint);
        return URI.create(protocol + "://" + endPoint);
    }

    // Add this helper method for SSE-C:
    static String calculateMD5(final String customerKey) {
        return Md5Utils.md5AsBase64(BinaryUtils.fromBase64(customerKey));
    }

    private static ClientOverrideConfiguration getClientConfiguration(Properties prop) {
        int maxErrorRetry = Integer.parseInt(prop.getProperty(S3Constants.S3_MAX_ERR_RETRY));
        int connectionTimeOut = Integer.parseInt(prop.getProperty(S3Constants.S3_CONN_TIMEOUT));
        String encryptionType = prop.getProperty(S3Constants.S3_ENCRYPTION);

        ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();

        builder.retryStrategy(b -> b.maxAttempts(maxErrorRetry));
        builder.apiCallTimeout(Duration.ofMillis(connectionTimeOut));

        if (S3Constants.S3_ENCRYPTION_SSE_KMS.equals(encryptionType)) {
            builder.putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create());
        }
        return builder.build();
    }

    private static SdkHttpClient getSdkHttpClient(Properties prop) {
        HttpClientConfig config = new HttpClientConfig(prop);
        final ApacheHttpClient.Builder builder = ApacheHttpClient.builder();

        builder.connectionTimeout(Duration.ofMillis(config.connectionTimeout))
                .socketTimeout(Duration.ofMillis(config.socketTimeout))
                .maxConnections(config.maxConnections);

        if (config.proxyHost != null && !config.proxyHost.isEmpty() && config.proxyPort != null && !config.proxyPort.isEmpty()) {
            String protocol = "http".equalsIgnoreCase(config.protocol) ? "http" : config.protocol;
            builder.proxyConfiguration(
                    ProxyConfiguration.builder()
                            .scheme(protocol)
                            .endpoint(URI.create(protocol + "://" + config.proxyHost + ":" + config.proxyPort))
                            .build()
            );
        }

        return builder.build();
    }

    private static SdkAsyncHttpClient getSdkAsyncHttpClient(Properties prop) {
        HttpClientConfig config = new HttpClientConfig(prop);
        final NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        builder.connectionTimeout(Duration.ofMillis(config.connectionTimeout))
                .readTimeout(Duration.ofMillis(config.socketTimeout))
                .writeTimeout(Duration.ofMillis(config.socketTimeout))
                .maxConcurrency(config.maxConnections);

        if (config.proxyHost != null && !config.proxyHost.isEmpty() && config.proxyPort != null && !config.proxyPort.isEmpty()) {
            String protocol = HTTPS.equalsIgnoreCase(config.protocol) ? HTTPS : config.protocol;
            builder.proxyConfiguration(
                    software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder()
                            .host(config.proxyHost)
                            .port(Integer.parseInt(config.proxyPort))
                            .scheme(protocol)
                            .build()
            );
        }
        return builder.build();
    }

    private static void deleteIfPossible(final File file) {
        boolean deleted = file.delete();
        if (!deleted) {
            LOG.warn("Could not delete {}", file.getAbsolutePath());
        }
    }

    @NotNull
    private static RemoteStorageMode getMode(@NotNull Properties props) {
        return props.getProperty(S3Constants.S3_END_POINT, "").contains("googleapis") ?
                RemoteStorageMode.GCP : RemoteStorageMode.S3;
    }

    private static String getDefaultRegion() {
        String region;
        final DefaultAwsRegionProviderChain regionProvider = new DefaultAwsRegionProviderChain();
        try {
            Region currentRegion = regionProvider.getRegion();
            region = currentRegion.id();
        } catch (Exception e) {
            throw SdkClientException.builder().message(
                    "parameter ["
                            + S3Constants.S3_REGION
                            + "] not configured and cannot be derived from environment").build();
        }
        return region;
    }

    @NotNull
    private static AwsCredentialsProvider getAwsCredentials(Properties prop) {
        String accessKey = prop.getProperty(S3Constants.ACCESS_KEY);
        String secretKey = prop.getProperty(S3Constants.SECRET_KEY);
        if (StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey)) {
            LOG.info("Configuring Amazon Client from environment");
            return DefaultCredentialsProvider.builder().build();
        } else {
            LOG.info("Configuring Amazon Client from property file.");
            final AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
            return StaticCredentialsProvider.create(credentials);
        }
    }

    private static void configureBuilder(final S3BaseClientBuilder builder, final Properties prop, final boolean accReq) {
        builder.credentialsProvider(getAwsCredentials(prop));
        builder.overrideConfiguration(getClientConfiguration(prop));

        // region is mandatory even with endpointOverride
        String region = getRegion(prop);
        builder.region(Region.of(region));

        builder.endpointOverride(getEndPointUri(prop, accReq, region));
        builder.crossRegionAccessEnabled(Boolean.parseBoolean(prop.getProperty(S3Constants.S3_CROSS_REGION_ACCESS)));
    }

    // Helper class to hold common Http config
    private static class HttpClientConfig {
        final int connectionTimeout;
        final int socketTimeout;
        final int maxConnections;
        final String protocol;
        final String proxyHost;
        final String proxyPort;

        HttpClientConfig(Properties prop) {
            this.connectionTimeout = Integer.parseInt(prop.getProperty(S3Constants.S3_CONN_TIMEOUT));
            this.socketTimeout = Integer.parseInt(prop.getProperty(S3Constants.S3_SOCK_TIMEOUT));
            this.maxConnections = Integer.parseInt(prop.getProperty(S3Constants.S3_MAX_CONNS));
            this.protocol = prop.getProperty(S3Constants.S3_CONN_PROTOCOL);
            this.proxyHost = prop.getProperty(S3Constants.PROXY_HOST);
            this.proxyPort = prop.getProperty(S3Constants.PROXY_PORT);
        }
    }
}
