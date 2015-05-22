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
package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import java.io.IOException;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.data.CachingDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Test {@link org.apache.jackrabbit.core.data.CachingDataStore} with
 * {@link org.apache.jackrabbit.core.data.CachingDataStore#setTouchAsync(boolean) set to true. It requires
 * to pass aws config file via system property. For e.g.
 * -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties
 */
public class TestS3DSAsyncTouch extends TestS3Ds {

    protected static final Logger LOG = LoggerFactory.getLogger(TestS3DSAsyncTouch.class);

    public TestS3DSAsyncTouch() throws IOException {

    }

    @Override
    protected CachingDataStore createDataStore() throws RepositoryException {
        S3DataStore s3ds = new S3DataStore();
        s3ds.setProperties(props);
        s3ds.setTouchAsync(true);
        s3ds.setSecret("123456");
        s3ds.init(dataStoreDir);
        s3ds.updateModifiedDateOnAccess(System.currentTimeMillis() + 50 * 1000);
        sleep(1000);
        return s3ds;
    }
}


