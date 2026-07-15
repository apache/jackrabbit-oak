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
package org.apache.jackrabbit.oak.segment.azure.v8;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.AZURE_CLIENT_ID;
import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.AZURE_CLIENT_SECRET;
import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.AZURE_SECRET_KEY;
import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.AZURE_TENANT_ID;

public class AzureStorageCredentialManagerV8 implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(AzureStorageCredentialManagerV8.class);
    private static final String AZURE_DEFAULT_SCOPE = "https://storage.azure.com/.default";
    private static final long TOKEN_REFRESHER_INITIAL_DELAY = 45L;
    private static final long TOKEN_REFRESHER_DELAY = 1L;
    private ClientSecretCredential clientSecretCredential;
    private AccessToken accessToken;
    private StorageCredentialsToken storageCredentialsToken;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    public StorageCredentials getStorageCredentialsFromEnvironment(@NotNull String accountName, @NotNull Environment environment) {
        final String clientId = environment.getVariable(AZURE_CLIENT_ID);
        final String clientSecret = environment.getVariable(AZURE_CLIENT_SECRET);
        final String tenantId = environment.getVariable(AZURE_TENANT_ID);

        if (StringUtils.isNoneBlank(clientId, clientSecret, tenantId)) {
            try {
                return getStorageCredentialAccessTokenFromServicePrincipals(accountName, clientId, clientSecret, tenantId);
            } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
                log.error("Error occurred while connecting to Azure Storage using service principals: ", e);
                throw new IllegalArgumentException(
                        "Could not connect to the Azure Storage. Please verify if AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and AZURE_TENANT_ID environment variables are correctly set!");
            }
        }

        log.warn("AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and AZURE_TENANT_ID environment variables empty or missing. Switching to authentication with AZURE_SECRET_KEY.");

        String key = environment.getVariable(AZURE_SECRET_KEY);
        try {
            return new StorageCredentialsAccountAndKey(accountName, key);
        } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
            log.error("Error occurred while connecting to Azure Storage using secret key: ", e);
            throw new IllegalArgumentException(
                    "Could not connect to the Azure Storage. Please verify if AZURE_SECRET_KEY environment variable is correctly set!");
        }
    }

    public StorageCredentials getStorageCredentialAccessTokenFromServicePrincipals(String accountName, String clientId, String clientSecret, String tenantId) {
        boolean isAccessTokenGenerated = false;
        if (accessToken == null) {
            clientSecretCredential = new ClientSecretCredentialBuilder()
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .tenantId(tenantId)
                    .build();
            accessToken = clientSecretCredential.getTokenSync(new TokenRequestContext().addScopes(AZURE_DEFAULT_SCOPE));
            if (accessToken == null || StringUtils.isBlank(accessToken.getToken())) {
                throw new IllegalArgumentException("Could not connect to azure storage, access token is null or empty");
            }
            storageCredentialsToken = new StorageCredentialsToken(accountName, accessToken.getToken());
            isAccessTokenGenerated = true;
        }
        Objects.requireNonNull(storageCredentialsToken, "storageCredentialsToken cannot be null");

        // start refresh token executor only when the access token is first generated
        if (isAccessTokenGenerated) {
            log.info("starting refresh token task at: {}", OffsetDateTime.now());
            TokenRefresher tokenRefresher = new TokenRefresher();
            executorService.scheduleWithFixedDelay(tokenRefresher, TOKEN_REFRESHER_INITIAL_DELAY, TOKEN_REFRESHER_DELAY, TimeUnit.MINUTES);
        }
        return storageCredentialsToken;
    }

    /**
     * This class represents a token refresher responsible for ensuring the validity of the access token used for azure AD authentication.
     * The access token generated by the Azure client is valid for 1 hour only. Therefore, this class periodically checks the validity
     * of the access token and refreshes it if necessary. The refresh is triggered when the current access token is about to expire,
     * defined by a threshold of 5 minutes from the current time. This threshold is similar to what is being used in azure identity library to
     * generate a new token
     */
    private class TokenRefresher implements Runnable {
        @Override
        public void run() {
            try {
                log.debug("Checking for azure access token expiry at: {}", LocalDateTime.now());
                OffsetDateTime tokenExpiryThreshold = OffsetDateTime.now().plusMinutes(5);
                if (accessToken.getExpiresAt() != null && accessToken.getExpiresAt().isBefore(tokenExpiryThreshold)) {
                    log.info("Access token is about to expire (5 minutes or less) at: {}. New access token will be generated",
                            accessToken.getExpiresAt().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    AccessToken newToken = clientSecretCredential.getTokenSync(new TokenRequestContext().addScopes(AZURE_DEFAULT_SCOPE));
                    log.info("New azure access token generated at: {}", LocalDateTime.now());
                    if (newToken == null || StringUtils.isBlank(newToken.getToken())) {
                        log.error("New access token is null or empty");
                        return;
                    }
                    // update access token with newly generated token
                    accessToken = newToken;
                    storageCredentialsToken.updateToken(accessToken.getToken());
                }
            } catch (Exception e) {
                log.error("Error while acquiring new access token: ", e);
            }
        }
    }

    @Override
    public void close() {
        new ExecutorCloser(executorService).close();
        log.info("Access token refresh executor shutdown completed");
    }
}
