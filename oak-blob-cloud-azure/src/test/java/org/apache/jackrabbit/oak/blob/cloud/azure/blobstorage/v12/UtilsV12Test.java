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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import com.azure.storage.common.policy.RequestRetryOptions;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for UtilsV12 — connection-string construction, auth priority, proxy options, and retry config.
 */
public class UtilsV12Test {

    @Test
    public void getConnectionStringForSas_withBlobEndpoint_usesBlobEndpointFormat() {
        String result = UtilsV12.getConnectionStringForSas("mySas", "https://myaccount.blob.core.windows.net", "myaccount");
        assertTrue(result.startsWith("BlobEndpoint=https://myaccount.blob.core.windows.net"));
        assertTrue(result.contains("SharedAccessSignature=mySas"));
    }

    @Test
    public void getConnectionStringForSas_noBlobEndpoint_usesAccountNameFormat() {
        String result = UtilsV12.getConnectionStringForSas("mySas", "", "myaccount");
        assertTrue(result.startsWith("AccountName=myaccount"));
        assertTrue(result.contains("SharedAccessSignature=mySas"));
    }

    @Test
    public void getConnectionString_withBlobEndpoint_includesEndpointInString() {
        String result = UtilsV12.getConnectionString("acc", "key123", "https://custom.endpoint.net");
        assertTrue(result.contains("AccountName=acc"));
        assertTrue(result.contains("AccountKey=key123"));
        assertTrue(result.contains("BlobEndpoint=https://custom.endpoint.net"));
    }

    @Test
    public void getConnectionString_noBlobEndpoint_omitsBlobEndpointField() {
        String result = UtilsV12.getConnectionString("acc", "key123", null);
        assertTrue(result.contains("AccountName=acc"));
        assertTrue(result.contains("AccountKey=key123"));
        assertFalse(result.contains("BlobEndpoint"));
    }

    @Test
    public void getConnectionString_emptyEndpoint_omitsBlobEndpointField() {
        String result = UtilsV12.getConnectionString("acc", "key123", "");
        assertFalse(result.contains("BlobEndpoint"));
    }

    /**
     * Connection string takes priority over SAS and account key.
     */
    @Test
    public void getConnectionStringFromProperties_explicitConnectionString_takesPriority() {
        Properties p = new Properties();
        p.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING, "explicit-connection-string");
        p.setProperty(AzureConstantsV12.AZURE_SAS, "should-not-be-used");
        assertEquals("explicit-connection-string", UtilsV12.getConnectionStringFromProperties(p));
    }

    /**
     * SAS URI is used when no explicit connection string is present.
     */
    @Test
    public void getConnectionStringFromProperties_sasUri_usedWhenNoConnectionString() {
        Properties p = new Properties();
        p.setProperty(AzureConstantsV12.AZURE_SAS, "mySas");
        p.setProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, "acc");
        String result = UtilsV12.getConnectionStringFromProperties(p);
        assertTrue(result.contains("mySas"));
    }

    /**
     * Falls back to account name + key when neither connection string nor SAS is set.
     */
    @Test
    public void getConnectionStringFromProperties_accountKey_fallbackWhenNoSas() {
        Properties p = new Properties();
        p.setProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, "acc");
        p.setProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_KEY, "key123");
        String result = UtilsV12.getConnectionStringFromProperties(p);
        assertTrue(result.contains("AccountName=acc"));
        assertTrue(result.contains("AccountKey=key123"));
    }

    @Test
    public void computeProxyOptions_strings_hostAndPortSet_returnsProxyOptions() {
        assertNotNull(UtilsV12.computeProxyOptions("proxy.example.com", "8080"));
    }

    @Test
    public void computeProxyOptions_strings_nullHost_returnsNull() {
        assertNull(UtilsV12.computeProxyOptions(null, "8080"));
    }

    @Test
    public void computeProxyOptions_strings_nullPort_returnsNull() {
        assertNull(UtilsV12.computeProxyOptions("proxy.example.com", null));
    }

    @Test
    public void computeProxyOptions_strings_emptyHost_returnsNull() {
        assertNull(UtilsV12.computeProxyOptions("", "8080"));
    }

    @Test
    public void computeProxyOptions_strings_bothNull_returnsNull() {
        assertNull(UtilsV12.computeProxyOptions(null, null));
    }

    /**
     * A negative retry count means "use SDK defaults" — return null so the SDK applies its own policy.
     */
    @Test
    public void getRetryOptions_negativeCount_returnsNull() {
        assertNull(UtilsV12.getRetryOptions("-1", null, null));
    }

    /**
     * Zero retries → fixed policy with maxTries=1 (no retry).
     */
    @Test
    public void getRetryOptions_zeroRetries_returnsNonNull() {
        assertNotNull(UtilsV12.getRetryOptions("0", null, null));
    }

    /**
     * Positive retry count → exponential policy.
     */
    @Test
    public void getRetryOptions_positiveCount_returnsNonNull() {
        assertNotNull(UtilsV12.getRetryOptions("3", null, null));
    }

    /**
     * A secondary location alone (no explicit retry count) must not silently disable
     * secondary-location failover — options should still be built, using the SDK's default
     * retry count, with the secondary host set.
     */
    @Test
    public void getRetryOptions_negativeCountWithSecondaryLocation_returnsOptionsWithSecondaryHost() {
        RequestRetryOptions options = UtilsV12.getRetryOptions("-1", null, "https://account-secondary.blob.core.windows.net");
        assertNotNull(options);
        assertEquals("https://account-secondary.blob.core.windows.net", options.getSecondaryHost());
        assertEquals(4, options.getMaxTries());
    }

    @Test(expected = IOException.class)
    public void readConfig_nonExistentFile_throwsIOException() throws IOException {
        UtilsV12.readConfig("/tmp/does-not-exist-" + System.nanoTime() + ".properties");
    }
}
