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
package org.apache.jackrabbit.oak.security.user;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.osgi.service.component.annotations.Component;

@Component(
        service = UserAuthenticationFactory.class,
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.security.user.UserAuthenticationFactoryImpl")
public class UserAuthenticationFactoryImpl implements UserAuthenticationFactory {

    @Nonnull
    @Override
    public Authentication getAuthentication(@Nonnull UserConfiguration userConfiguration, @Nonnull Root root, @Nullable String userId) {
        return new UserAuthentication(userConfiguration, root, userId);
    }
}
