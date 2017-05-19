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

/**
 * Defined Amazon S3 constants.
 */
public final class S3Constants {

    /**
     * Amazon aws access key.
     */
    public static final String ACCESS_KEY = "accessKey";

    /**
     * Amazon aws secret key.
     */
    public static final String SECRET_KEY = "secretKey";
    
    /**
     * Amazon S3 Http connection timeout.
     */
    public static final String S3_CONN_TIMEOUT = "connectionTimeout";
    
    /**
     * Amazon S3  socket timeout.
     */
    public static final String S3_SOCK_TIMEOUT = "socketTimeout";
    
    /**
     * Amazon S3  maximum connections to be used.
     */
    public static final String S3_MAX_CONNS = "maxConnections";
    
    /**
     * Amazon S3  maximum retries.
     */
    public static final String S3_MAX_ERR_RETRY = "maxErrorRetry";

    /**
     * Amazon aws S3 bucket.
     */
    public static final String S3_BUCKET = "s3Bucket";

    /**
     * Amazon aws S3 bucket (alternate property name).
     */
    public static final String S3_CONTAINER = "container";

    /**
     * Amazon aws S3 region.
     */
    public static final String S3_REGION = "s3Region";
    
    /**
     * Amazon aws S3 region.
     */
    public static final String S3_END_POINT = "s3EndPoint";
    
    /**
     * Constant for S3 Connector Protocol
     */
    public static final String S3_CONN_PROTOCOL = "s3ConnProtocol";

    /**
     * Constant to rename keys
     */
    public static final String S3_RENAME_KEYS = "s3RenameKeys";

    /**
     * Constant to rename keys
     */
    public static final String S3_WRITE_THREADS = "writeThreads";
    
    /**
     * Constant to enable encryption in S3.
     */
    public static final String S3_ENCRYPTION = "s3Encryption";

    /**
     * Constant for no encryption. it is default.
     */
    public static final String S3_ENCRYPTION_NONE = "NONE";

    /**
     *  Constant to set SSE_S3 encryption.
     */
    public static final String S3_ENCRYPTION_SSE_S3 = "SSE_S3";

    /**
     *  Constant to set proxy host.
     */
    public static final String PROXY_HOST = "proxyHost";

    /**
     *  Constant to set proxy port.
     */
    public static final String PROXY_PORT = "proxyPort";

    /**
     * private constructor so that class cannot initialized from outside.
     */
    private S3Constants() {

    }

}
