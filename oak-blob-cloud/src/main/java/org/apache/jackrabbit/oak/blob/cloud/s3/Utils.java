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
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;

/**
 * Amazon S3 utilities.
 */
public final class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static final String DEFAULT_CONFIG_FILE = "aws.properties";

    private static final String DELETE_CONFIG_SUFFIX = ";burn";

    /**
     * The default value AWS bucket region.
     */
    public static final String DEFAULT_AWS_BUCKET_REGION = "us-standard";

    /**
     * constants to define endpoint to various AWS region
     */
    public static final String AWSDOTCOM = "amazonaws.com";

    public static final String S3 = "s3";

    public static final String DOT = ".";

    public static final String DASH = "-";

    /**
     * private constructor so that class cannot initialized from outside.
     */
    private Utils() {

    }

    /**
     * Create AmazonS3Client from properties.
     * 
     * @param prop properties to configure @link {@link AmazonS3Client}
     * @return {@link AmazonS3Client}
     */
    public static AmazonS3Client openService(final Properties prop) {
        String accessKey = prop.getProperty(S3Constants.ACCESS_KEY);
        String secretKey = prop.getProperty(S3Constants.SECRET_KEY);
        AmazonS3Client s3service = null;
        if (StringUtils.isNullOrEmpty(accessKey)
                || StringUtils.isNullOrEmpty(secretKey)) {
            LOG.info("Configuring Amazon Client from environment");
            s3service = new AmazonS3Client(getClientConfiguration(prop));
        } else {
            LOG.info("Configuring Amazon Client from property file.");
            AWSCredentials credentials = new BasicAWSCredentials(accessKey,
                    secretKey);
            s3service = new AmazonS3Client(credentials,
                    getClientConfiguration(prop));
        }
        String region = prop.getProperty(S3Constants.S3_REGION);
        String endpoint = null;
        String propEndPoint = prop.getProperty(S3Constants.S3_END_POINT);
        if ((propEndPoint != null) & !"".equals(propEndPoint)) {
            endpoint = propEndPoint;
        } else {
            if (StringUtils.isNullOrEmpty(region)) {
                com.amazonaws.regions.Region s3Region = Regions.getCurrentRegion();
                if (s3Region != null) {
                    region = s3Region.getName();
                } else {
                    throw new AmazonClientException(
                            "parameter ["
                                    + S3Constants.S3_REGION
                                    + "] not configured and cannot be derived from environment");
                }
            }
            if (DEFAULT_AWS_BUCKET_REGION.equals(region)) {
                endpoint = S3 + DOT + AWSDOTCOM;
            } else if (Region.EU_Ireland.toString().equals(region)) {
                endpoint = "s3-eu-west-1" + DOT + AWSDOTCOM;
            } else {
                endpoint = S3 + DASH + region + DOT + AWSDOTCOM;
            }
        }
        /*
         * setting endpoint to remove latency of redirection. If endpoint is
         * not set, invocation first goes us standard region, which
         * redirects it to correct location.
         */
        s3service.setEndpoint(endpoint);
        LOG.info("S3 service endpoint [{}] ", endpoint);
        return s3service;
    }

    /**
     * Delete S3 bucket. This method first deletes all objects from bucket and
     * then delete empty bucket.
     * 
     * @param bucketName the bucket name.
     */
    public static void deleteBucket(final String bucketName) throws IOException {
        Properties prop = readConfig(DEFAULT_CONFIG_FILE);
        AmazonS3 s3service = openService(prop);
        ObjectListing prevObjectListing = s3service.listObjects(bucketName);
        while (true) {
            for (S3ObjectSummary s3ObjSumm : prevObjectListing.getObjectSummaries()) {
                s3service.deleteObject(bucketName, s3ObjSumm.getKey());
            }
            if (!prevObjectListing.isTruncated()) {
                break;
            }
            prevObjectListing = s3service.listNextBatchOfObjects(prevObjectListing);
        }
        s3service.deleteBucket(bucketName);
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

    private static void deleteIfPossible(final File file) {
        boolean deleted = file.delete();
        if (!deleted) {
            LOG.warn("Could not delete " + file.getAbsolutePath());
        }
    }

    private static ClientConfiguration getClientConfiguration(Properties prop) {
        int connectionTimeOut = Integer.parseInt(prop.getProperty(S3Constants.S3_CONN_TIMEOUT));
        int socketTimeOut = Integer.parseInt(prop.getProperty(S3Constants.S3_SOCK_TIMEOUT));
        int maxConnections = Integer.parseInt(prop.getProperty(S3Constants.S3_MAX_CONNS));
        int maxErrorRetry = Integer.parseInt(prop.getProperty(S3Constants.S3_MAX_ERR_RETRY));

        String protocol = prop.getProperty(S3Constants.S3_CONN_PROTOCOL);
        String proxyHost = prop.getProperty(S3Constants.PROXY_HOST);
        String proxyPort = prop.getProperty(S3Constants.PROXY_PORT);

        ClientConfiguration cc = new ClientConfiguration();

        if (protocol != null && protocol.equalsIgnoreCase("http")) {
            cc.setProtocol(Protocol.HTTP);
        }

        if (proxyHost != null && !proxyHost.isEmpty()) {
            cc.setProxyHost(proxyHost);
        }

        if (proxyPort != null && !proxyPort.isEmpty()) {
            cc.setProxyPort(Integer.parseInt(proxyPort));
        }

        cc.setConnectionTimeout(connectionTimeOut);
        cc.setSocketTimeout(socketTimeOut);
        cc.setMaxConnections(maxConnections);
        cc.setMaxErrorRetry(maxErrorRetry);
        return cc;
    }

    public static Map<String, Object> asMap(Properties props) {
        Map<String, Object> map = Maps.newHashMap();
        for (Object key : props.keySet()) {
            map.put((String)key, props.get(key));
        }
        return map;
    }
}
