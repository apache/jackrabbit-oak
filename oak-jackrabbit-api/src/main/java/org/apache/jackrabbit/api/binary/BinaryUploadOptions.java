/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.api.binary;

import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Specifies the options to be used when requesting direct upload URIs via
 * {@link org.apache.jackrabbit.api.JackrabbitValueFactory#initiateBinaryUpload(long, int, BinaryUploadOptions)}.
 * <p>
 * To specify upload options, obtain a {@link BinaryUploadOptionsBuilder}
 * via the {@link #builder()} method, then specify the options desired and
 * get the object via {@link BinaryUploadOptionsBuilder#build()}.
 * <p>
 * If no options are needed, use {@link BinaryUploadOptions#DEFAULT} which
 * instructs the implementation to use the service provider default behavior.
 */
@ProviderType
public final class BinaryUploadOptions {
    private final boolean domainOverrideIgnore;

    private BinaryUploadOptions(boolean domainOverrideIgnore) {
        this.domainOverrideIgnore = domainOverrideIgnore;
    }

    /**
     * Provides a default instance of this class.  This instance enforces the
     * proper default behaviors for the options.
     */
    public static final BinaryUploadOptions DEFAULT =
            BinaryUploadOptions.builder().build();

    /**
     * Indicates whether the option to ignore any configured domain override
     * setting has been specified.
     *
     * @return true if the domain override should be ignored; false otherwise.
     * The default behavior is {@code false}, meaning that any configured domain
     * override setting should be honored.
     */
    public boolean isDomainOverrideIgnored() {
        return domainOverrideIgnore;
    }

    /**
     * Returns a {@link BinaryUploadOptionsBuilder} instance to be used for
     * creating an instance of this class.
     *
     * @return A builder instance.
     */
    @NotNull
    public static BinaryUploadOptionsBuilder builder() {
        return new BinaryUploadOptionsBuilder();
    }

    /**
     * Used to build an instance of {@link BinaryUploadOptions} with the options
     * set as desired by the caller.
     */
    public static final class BinaryUploadOptionsBuilder {
        private boolean domainOverrideIgnore = false;

        private BinaryUploadOptionsBuilder() { }

        /**
         * Sets the option to ignore any configured domain override setting.
         *
         * The default value of this option is false, meaning that any
         * configured domain override setting should be honored when generating
         * signed upload URIs.  Setting this value to true will indicate that
         * the signed upload URIs being generated should not honor any
         * configured domain override setting.
         *
         * @param domainOverrideIgnore true to ignore any configured domain
         *                             override setting, false otherwise.
         * @return the calling instance.
         */
        public BinaryUploadOptionsBuilder withDomainOverrideIgnore(boolean domainOverrideIgnore) {
            this.domainOverrideIgnore = domainOverrideIgnore;
            return this;
        }

        /**
         * Construct a {@link BinaryUploadOptions} instance with the
         * properties specified to the builder.
         *
         * @return A new {@link BinaryUploadOptions} instance built with the
         *         properties specified to the builder.
         */
        public BinaryUploadOptions build() {
            return new BinaryUploadOptions(domainOverrideIgnore);
        }
    }
}
