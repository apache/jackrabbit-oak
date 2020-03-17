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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class UtilsTest {

    @Test
    public void testConnectionStringIsBasedOnSAS() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint");
        String connectionString = Utils.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("BlobEndpoint=%s;SharedAccessSignature=%s", "endpoint", "sas"));
    }

    @Test
    public void testConnectionStringIsBasedOnAccessKeyIfSASMissing() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "accessKey");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "secretKey");

        String connectionString = Utils.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s","accessKey","secretKey"));
    }

    @Test
    public void testConnectionStringSASIsPriority() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint");

        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "accessKey");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "secretKey");

        String connectionString = Utils.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("BlobEndpoint=%s;SharedAccessSignature=%s", "endpoint", "sas"));
    }


}
