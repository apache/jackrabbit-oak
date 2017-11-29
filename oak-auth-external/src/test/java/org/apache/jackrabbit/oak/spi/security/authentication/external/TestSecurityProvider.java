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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalPrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;

public class TestSecurityProvider {

    public static SecurityProvider newTestSecurityProvider(@Nonnull ConfigurationParameters configuration,
            @Nonnull ExternalPrincipalConfiguration externalPrincipalConfiguration) {
        SecurityProvider delegate = new SecurityProviderBuilder().with(configuration).build();

        PrincipalConfiguration principalConfiguration = delegate.getConfiguration(PrincipalConfiguration.class);
        if (!(principalConfiguration instanceof CompositePrincipalConfiguration)) {
            throw new IllegalStateException();
        } else {
            externalPrincipalConfiguration.setSecurityProvider(delegate);
            CompositePrincipalConfiguration composite = (CompositePrincipalConfiguration) principalConfiguration;
            PrincipalConfiguration defConfig = composite.getDefaultConfig();
            composite.addConfiguration(externalPrincipalConfiguration);
            composite.addConfiguration(defConfig);
        }
        return delegate;
    }
}