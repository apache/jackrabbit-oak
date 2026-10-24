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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFileSupplier;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Prefers IMDSv2 and retries with the standard AWS policy if needed.
 * AWS system and environment settings take precedence.
 */
final class ImdsV2CredentialsProvider implements AwsCredentialsProvider, SdkAutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ImdsV2CredentialsProvider.class);

    private final DefaultCredentialsProvider imdsV2Only;
    private final DefaultCredentialsProvider fallback;
    private final AtomicBoolean useFallback = new AtomicBoolean();
    private volatile ProfileCache cachedProfiles;

    ImdsV2CredentialsProvider() {
        ProfileFileSupplier profiles = ProfileFileSupplier.defaultSupplier();
        imdsV2Only = DefaultCredentialsProvider.builder()
                .profileFile(() -> withImdsV1Disabled(profiles.get()))
                .build();
        fallback = DefaultCredentialsProvider.builder().build();
    }

    ProfileFile withImdsV1Disabled(ProfileFile profiles) {
        String profileName = ProfileFileSystemSetting.AWS_PROFILE.getStringValueOrThrow();
        ProfileCache cached = cachedProfiles;
        if (cached != null && cached.source() == profiles && cached.profileName().equals(profileName)) {
            return cached.augmented();
        }
        Set<String> profileNames = new LinkedHashSet<>(profiles.profiles().keySet());
        profileNames.add(profileName);
        StringBuilder overrideContent = new StringBuilder();
        // The SDK reads IMDS settings from the source profile when resolving nested roles.
        for (String name : profileNames) {
            overrideContent.append("[profile ").append(name).append("]\nec2_metadata_v1_disabled=true\n");
        }
        ProfileFile override = ProfileFile.builder()
                .content(overrideContent.toString())
                .type(ProfileFile.Type.CONFIGURATION)
                .build();
        ProfileFile augmented = ProfileFile.aggregator().addFile(override).addFile(profiles).build();
        cachedProfiles = new ProfileCache(profiles, profileName, augmented);
        return augmented;
    }

    private record ProfileCache(ProfileFile source, String profileName, ProfileFile augmented) { }

    @Override
    public AwsCredentials resolveCredentials() {
        if (useFallback.get()) {
            return fallback.resolveCredentials();
        }
        try {
            return imdsV2Only.resolveCredentials();
        } catch (SdkClientException failure) {
            AwsCredentials credentials = fallback.resolveCredentials();
            if (useFallback.compareAndSet(false, true)) {
                LOG.error("Credential lookup recovered after retrying with standard AWS settings. "
                        + "Set EC2 HttpTokens=required to prevent IMDSv1 fallback.");
            }
            return credentials;
        }
    }

    @Override
    public void close() {
        try {
            imdsV2Only.close();
        } finally {
            fallback.close();
        }
    }
}
