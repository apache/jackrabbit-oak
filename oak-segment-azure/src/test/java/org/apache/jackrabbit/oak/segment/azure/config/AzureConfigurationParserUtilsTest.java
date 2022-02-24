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
package org.apache.jackrabbit.oak.segment.azure.config;

import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils;
import org.junit.Test;

public class AzureConfigurationParserUtilsTest {

    @Test
    public void testParseConnectionDetailsFromCustomConnection() {
        String connStr = "DefaultEndpointsProtocol=https;"
            + "AccountName=myaccount;"
            + "AccountKey=mykey==;"
            + "BlobEndpoint=http://127.0.0.1:32806/myaccount;";

        String conn = connStr
            + "ContainerName=oak-test;"
            + "Directory=repository";

        assertTrue("Should be a custom Azure connection string", AzureConfigurationParserUtils.isCustomAzureConnectionString(conn));

        Map<String, String> config = AzureConfigurationParserUtils.parseAzureConfigurationFromCustomConnection(conn);
        assertEquals(connStr, config.get(KEY_CONNECTION_STRING));
        assertEquals("oak-test", config.get(KEY_CONTAINER_NAME));
        assertEquals("repository", config.get(KEY_DIR));
    }

    @Test
    public void testParseConnectionDetailsFromCustomConnectionShuffledKeys() {
        String conn = "Directory=repository;"
            + "DefaultEndpointsProtocol=https;" 
            + "ContainerName=oak-test;" 
            + "AccountName=myaccount;"
            + "BlobEndpoint=http://127.0.0.1:32806/myaccount;" 
            + "AccountKey=mykey==";

        assertTrue("Should be a custom Azure connection string", AzureConfigurationParserUtils.isCustomAzureConnectionString(conn));
        
        String azureConn = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey==;BlobEndpoint=http://127.0.0.1:32806/myaccount;";

        Map<String, String> config = AzureConfigurationParserUtils.parseAzureConfigurationFromCustomConnection(conn);
        assertEquals(azureConn, config.get(KEY_CONNECTION_STRING));
        assertEquals("oak-test", config.get(KEY_CONTAINER_NAME));
        assertEquals("repository", config.get(KEY_DIR));
    }

    @Test
    public void testParseConnectionDetailsFromSASConnectionString() {
        String connStr = "DefaultEndpointsProtocol=https;"
            + "AccountName=myaccount;"
            + "SharedAccessSignature=mySasToken==;"
            + "BlobEndpoint=http://127.0.0.1:32806/myaccount;";

        String conn = connStr
            + "ContainerName=oak-test;"
            + "Directory=repository";

        assertTrue("Should be a custom Azure connection string", AzureConfigurationParserUtils.isCustomAzureConnectionString(conn));

        Map<String, String> config = AzureConfigurationParserUtils.parseAzureConfigurationFromCustomConnection(conn);
        assertEquals(connStr, config.get(KEY_CONNECTION_STRING));
        assertEquals("oak-test", config.get(KEY_CONTAINER_NAME));
        assertEquals("repository", config.get(KEY_DIR));
        assertEquals("mySasToken==", config.get(KEY_SHARED_ACCESS_SIGNATURE));
    }

    @Test
    public void testParseConnectionDetailsFromUri() {
        String uri = "https://myaccount.blob.core.windows.net/oak-test/repository";
        assertFalse("Should not be a custom Azure connection", AzureConfigurationParserUtils.isCustomAzureConnectionString(uri));

        Map<String, String> config = AzureConfigurationParserUtils.parseAzureConfigurationFromUri(uri);

        assertEquals("myaccount", config.get(KEY_ACCOUNT_NAME));
        assertEquals("https://myaccount.blob.core.windows.net/oak-test", config.get(KEY_STORAGE_URI));
        assertEquals("repository", config.get(KEY_DIR));
    }

    @Test
    public void testParseConnectionDetailsFromSASUri() {
        String sasToken = "sig=qL%2Fi%2BP7J6S0sA8Ihc%2BKq75U5uJcnukpfktT2fm1ckXk%3D&se=2022-02-09T11%3A52%3A42Z&sv=2019-02-02&sp=rl&sr=c";
        String uri = "https://myaccount.blob.core.windows.net/oak-test/repository?" + sasToken;
        assertFalse("Should not be a custom Azure connection", AzureConfigurationParserUtils.isCustomAzureConnectionString(uri));

        Map<String, String> config = AzureConfigurationParserUtils.parseAzureConfigurationFromUri(uri);

        assertEquals("myaccount", config.get(KEY_ACCOUNT_NAME));
        assertEquals("https://myaccount.blob.core.windows.net/oak-test", config.get(KEY_STORAGE_URI));
        assertEquals("repository", config.get(KEY_DIR));
        assertEquals(sasToken, config.get(KEY_SHARED_ACCESS_SIGNATURE));
    }
}
