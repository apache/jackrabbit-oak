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

/**
 * Shared Azure configuration property name constants used by consumer modules
 * ({@code oak-run-commons}, {@code oak-jcr}, etc.) to configure {@link AzureDataStore}.
 * <p>
 * Property names are intentionally literal so this shared class does not force either
 * version-specific SDK package to be loaded.
 */
public final class AzureConstants {
    public static final String AZURE_V12_ENABLED_PROPERTY = "blob.azure.v12.enabled";

    public static final String AZURE_STORAGE_ACCOUNT_NAME = "accessKey";
    public static final String AZURE_STORAGE_ACCOUNT_KEY = "secretKey";
    public static final String AZURE_CONNECTION_STRING = "azureConnectionString";
    public static final String AZURE_SAS = "azureSas";
    public static final String AZURE_TENANT_ID = "tenantId";
    public static final String AZURE_CLIENT_ID = "clientId";
    public static final String AZURE_CLIENT_SECRET = "clientSecret";
    public static final String AZURE_BLOB_ENDPOINT = "azureBlobEndpoint";
    public static final String AZURE_BLOB_CONTAINER_NAME = "container";

    private AzureConstants() { }
}
