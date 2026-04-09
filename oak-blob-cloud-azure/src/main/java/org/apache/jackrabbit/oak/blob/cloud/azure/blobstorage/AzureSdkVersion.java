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

import java.util.Properties;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The single point of SDK version selection for Azure blob storage.
 * <p>
 * Resolves the active SDK version from configuration properties (preferred)
 * or the {@code blob.azure.v12.enabled} system property (fallback).
 */
public enum AzureSdkVersion {
    V8, V12;

    /**
     * Resolves the SDK version to use. Checks the given properties first;
     * falls back to the system property {@code blob.azure.v12.enabled}.
     */
    @NotNull
    public static AzureSdkVersion resolve(@Nullable Properties properties) {
        if (properties != null) {
            String configuredValue = properties.getProperty(AzureConstants.AZURE_V12_ENABLED_PROPERTY);
            if (configuredValue != null) {
                return Boolean.parseBoolean(configuredValue) ? V12 : V8;
            }
        }
        boolean sysProp = SystemPropertySupplier.create(AzureConstants.AZURE_V12_ENABLED_PROPERTY, false).get();
        return sysProp ? V12 : V8;
    }
}
