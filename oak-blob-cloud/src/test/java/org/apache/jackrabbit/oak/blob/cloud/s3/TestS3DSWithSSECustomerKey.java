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

import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.slf4j.Logger;

import static com.amazonaws.util.StringUtils.hasValue;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_ENCRYPTION_SSE_C;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants.S3_SSE_C_KEY;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Test S3DataStore operation with SSE_C encryption.
 * It requires to pass aws config file via system property  or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties
 *
 */
public class TestS3DSWithSSECustomerKey extends TestS3Ds {

        protected static final Logger LOG = getLogger(TestS3DSWithSSECustomerKey.class);

        @Override
        @Before
        public void setUp() throws Exception {
            super.setUp();
            String keyId = props.getProperty(S3_SSE_C_KEY);
            if (hasValue(keyId)) {
                props.setProperty(S3_ENCRYPTION, S3_ENCRYPTION_SSE_C);
                props.setProperty(S3_SSE_C_KEY, keyId);
            } else {
                LOG.info("SSE Customer Key ID not configured so ignoring test");
                throw new AssumptionViolatedException("SSE Customer key Id not configured");
            }
        }
}
