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
package org.apache.jackrabbit.oak.segment.azure;

import org.junit.Test;

import static org.junit.Assert.assertThrows;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.osgi.util.converter.Converters;

import java.util.HashMap;

public class AzurePersistenceManagerTest {

    @Test
    public void bothConnectionStringAndSasTokenNotConfiguredTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence(null, null, "accountName", "containerName", "aem", false, false));
    }

    @Test
    public void bothConnectionStringAndSasTokenBlankTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence("", "", "accountName", "containerName", "aem", false, false));
    }

    @Test
    public void connectionStringBlankAndSasTokenNullTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence("", null, "accountName", "containerName", "aem", false, false));
    }

    @Test
    public void connectionStringNullAndSasTokenBlankTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence(null, "", "accountName", "containerName", "aem", false, false));
    }

    @Test
    public void accountNameIsNullTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence("connectionString", "sasToken", null, "containerName", "aem", false, false));
    }

    @Test
    public void accountNameIsEmptyTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence("connectionString", "sasToken", "", "containerName", "aem", false, false));
    }

    @Test
    public void containerNameIsNullTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence("connectionString", "sasToken", "accountName", null, "aem", false, false));
    }

    @Test
    public void containerNameIsEmptyTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence("connectionString", "sasToken", "accountName", "", "aem", false, false));
    }

    @Test
    public void rootPrefixIsNullTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence("connectionString", "sasToken", "accountName", "containerName", null, false, false));
    }

    @Test
    public void rootPrefixIsEmptyTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistence("connectionString", "sasToken", "accountName", "containerName", "", false, false));
    }

    @Test
    public void servicePrincipalsAccountNameIsNullTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createPersistenceFromServicePrincipalCredentials(null, "containerName", "rootPrefix", "clientId", "clientSecret", "tenantId", false, false));
    }

    @Test
    public void servicePrincipalAccountNameIsEmptyTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createPersistenceFromServicePrincipalCredentials("", "containerName", "rootPrefix", "clientId", "clientSecret", "tenantId", false, false));
    }

    @Test
    public void servicePrincipalsContainerNameIsNullTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createPersistenceFromServicePrincipalCredentials("accountName", null, "rootPrefix", "clientId", "clientSecret", "tenantId", false, false));
    }

    @Test
    public void servicePrincipalsContainerNameIsEmptyTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createPersistenceFromServicePrincipalCredentials("accountName", "", "rootPrefix", "clientId", "clientSecret", "tenantId", false, false));
    }

    @Test
    public void servicePrincipalsRootPrefixIsNullTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createPersistenceFromServicePrincipalCredentials("accountName", "containerName", null, "clientId", "clientSecret", "tenantId", false, false));
    }

    @Test
    public void servicePrincipalsRootPrefixIsEmptyTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createPersistenceFromServicePrincipalCredentials("accountName", "containerName", "", "clientId", "clientSecret", "tenantId", false, false));
    }

    @Test
    public void createAzurePersistenceAllNullTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistenceFrom(null, null, null, "sasToken"));
    }

    @Test
    public void createAzurePersistenceFromEnvAllNullTest() {
        Environment env = new Environment();
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistenceFrom(null, null, null, env));
    }

    @Test
    public void createAzurePersistenceFromConfigurationAllNullTest() {
        assertThrows(IllegalArgumentException.class, () -> AzurePersistenceManager.createAzurePersistenceFrom(getConfiguration()));
    }

    private static Configuration getConfiguration() {
        return Converters.standardConverter()
                .convert(new HashMap<Object, Object>() {{
                    put("accountName", null);
                    put("accessKey", null);
                    put("connectionURL", "https://accounts.blob.core.windows.net");
                    put("sharedAccessSignature", "sharedAccessSignature");
                    put("clientId", "clientId");
                    put("clientSecret", "clientSecret");
                    put("tenantId", "tenantId");
                    put("blobEndpoint", "blobEndpoint");
                }})
                .to(Configuration.class);
    }

}