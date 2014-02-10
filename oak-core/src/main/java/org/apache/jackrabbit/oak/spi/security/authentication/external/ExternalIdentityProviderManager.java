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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * The external identity provider management.
 *
 * The default manager is registered as OSGi service and can also be retrieved via
 * {@link org.apache.jackrabbit.oak.spi.security.SecurityProvider#getConfiguration(Class)}
 */
public interface ExternalIdentityProviderManager {

    /**
     * Returns the registered identity provider with the given name.
     * @param name the provider name
     * @return the registered provider or {@code null}
     */
    @CheckForNull
    ExternalIdentityProvider getProvider(@Nonnull String name);
}