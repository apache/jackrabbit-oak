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

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3Config;

/**
 * Test S3DataStore operation with SSE_KMS encryption.
 * It requires to pass aws config file via system property  or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties
 *
 */
public class TestS3DSWithSSEKMS extends TestS3Ds {

        protected static final Logger LOG = LoggerFactory.getLogger(TestS3DSWithSSES3.class);

        @Override
        @Before
        public void setUp() throws Exception {
            super.setUp();
            props.setProperty(S3Constants.S3_ENCRYPTION, S3Constants.S3_ENCRYPTION_SSE_KMS);
            String kmsKey = props.getProperty(S3Constants.S3_SSE_KMS_KEYID);
            if (kmsKey != null) {
                props.remove(S3Constants.S3_SSE_KMS_KEYID);
            }
        }
}
