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

import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_ACCOUNT_NAME;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_CONNECTION_STRING;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_CONTAINER_NAME;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_DIR;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_STORAGE_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils;
import org.junit.Test;

public class AzureConfigurationParserUtilsTest {

    @Test
    public void testParseConnectionDetailsFromCustomConnection() {
        StringBuilder conn = new StringBuilder();
        StringBuilder connStr = new StringBuilder();
        connStr.append("DefaultEndpointsProtocol=https;");
        connStr.append("AccountName=myaccount;");
        connStr.append("AccountKey=mykey==;");
        connStr.append("BlobEndpoint=http://127.0.0.1:32806/myaccount;");

        conn.append(connStr);
        conn.append("ContainerName=oak-test;");
        conn.append("Directory=repository");

        assertTrue(AzureConfigurationParserUtils.isCustomAzureConnectionString(conn.toString()));

        Map<String, String> config = AzureConfigurationParserUtils.parseAzureConfigurationFromCustomConnection(conn.toString());
        assertEquals(connStr.toString(), config.get(KEY_CONNECTION_STRING));
        assertEquals("oak-test", config.get(KEY_CONTAINER_NAME));
        assertEquals("repository", config.get(KEY_DIR));
    }

    @Test
    public void testParseConnectionDetailsFromCustomConnectionShuffledKeys() {
        StringBuilder conn = new StringBuilder();
        conn.append("Directory=repository;");
        conn.append("DefaultEndpointsProtocol=https;");
        conn.append("ContainerName=oak-test;");
        conn.append("AccountName=myaccount;");
        conn.append("BlobEndpoint=http://127.0.0.1:32806/myaccount;");
        conn.append("AccountKey=mykey==");

        assertTrue(AzureConfigurationParserUtils.isCustomAzureConnectionString(conn.toString()));
        String azureConn = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey==;BlobEndpoint=http://127.0.0.1:32806/myaccount;";

        Map<String, String> config = AzureConfigurationParserUtils.parseAzureConfigurationFromCustomConnection(conn.toString());
        assertEquals(azureConn, config.get(KEY_CONNECTION_STRING));
        assertEquals("oak-test", config.get(KEY_CONTAINER_NAME));
        assertEquals("repository", config.get(KEY_DIR));
    }

    @Test
    public void testParseConnectionDetailsFromUri() {
        String uri = "https://myaccount.blob.core.windows.net/oak-test/repository";
        assertFalse(AzureConfigurationParserUtils.isCustomAzureConnectionString(uri));

        Map<String, String> config = AzureConfigurationParserUtils.parseAzureConfigurationFromUri(uri);

        assertEquals("myaccount", config.get(KEY_ACCOUNT_NAME));
        assertEquals("https://myaccount.blob.core.windows.net/oak-test", config.get(KEY_STORAGE_URI));
        assertEquals("repository", config.get(KEY_DIR));
    }
}
