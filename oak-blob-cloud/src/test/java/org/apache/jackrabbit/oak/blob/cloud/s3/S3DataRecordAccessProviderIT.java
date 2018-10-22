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
import java.io.IOException;
import java.net.URI;

import javax.net.ssl.HttpsURLConnection;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.AbstractDataRecordAccessProviderIT;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getFixtures;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3DataStore;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.junit.Assume.assumeTrue;

/**
 * The test is memory intensive and requires -Dtest.opts.memory=-Xmx2G
 */
public class S3DataRecordAccessProviderIT extends AbstractDataRecordAccessProviderIT {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static S3DataStore dataStore;

    @BeforeClass
    public static void setupDataStore() throws Exception {
        assumeTrue(isS3Configured() && !isNullOrEmpty(System.getProperty("test.opts.memory")));
        dataStore = (S3DataStore) getS3DataStore(getFixtures().get(0), S3DataStoreUtils.getS3Config(),
            homeDir.newFolder().getAbsolutePath());

        dataStore.setDirectDownloadURIExpirySeconds(expirySeconds);
        dataStore.setDirectUploadURIExpirySeconds(expirySeconds);
    }

    @Override
    protected ConfigurableDataRecordAccessProvider getDataStore() {
        return dataStore;
    }

    @Override
    protected DataRecord doGetRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        return ds.getRecord(identifier);
    }

    @Override
    protected void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        ((S3DataStore)ds).deleteRecord(identifier);
    }

    @Override
    protected long getProviderMaxPartSize() {
        return S3Backend.MAX_MULTIPART_UPLOAD_PART_SIZE;
    }

    @Override
    protected HttpsURLConnection getHttpsConnection(long length, URI uri) throws IOException {
        return S3DataStoreUtils.getHttpsConnection(length, uri);
    }
}
