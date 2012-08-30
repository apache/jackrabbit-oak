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

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfig;
import org.apache.jackrabbit.oak.spi.security.user.UserContext;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;

/**
 * UserContextImpl... TODO
 */
public class UserContextImpl implements UserContext {

    private final UserConfig config;

    // TODO add proper configuration
    public UserContextImpl() {
        this(new UserConfig("admin"));
    }

    public UserContextImpl(UserConfig config) {
        this.config = config;
    }

    @Override
    public UserConfig getConfig() {
        return config;
    }

    @Override
    public UserProvider getUserProvider(ContentSession contentSession, Root root) {
        return new UserProviderImpl(contentSession, root, config);
    }

    @Override
    public MembershipProvider getMembershipProvider(ContentSession contentSession, Root root) {
        return new MembershipProviderImpl(contentSession, root, config);
    }

    @Override
    public ValidatorProvider getUserValidatorProvider(CoreValueFactory valueFactory) {
        return new UserValidatorProvider(config);
    }
}