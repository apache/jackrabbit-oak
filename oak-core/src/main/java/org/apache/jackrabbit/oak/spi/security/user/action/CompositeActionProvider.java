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
package org.apache.jackrabbit.oak.spi.security.user.action;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;

/**
 * Aggregates a collection of {@link AuthorizableActionProvider}s into a single
 * provider.
 */
public class CompositeActionProvider implements AuthorizableActionProvider {

    private final Collection<? extends AuthorizableActionProvider> providers;

    public CompositeActionProvider(Collection<? extends AuthorizableActionProvider> providers) {
        this.providers = providers;
    }

    public CompositeActionProvider(AuthorizableActionProvider... providers) {
        this.providers = Arrays.asList(providers);
    }

    @Nonnull
    @Override
    public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
        List<AuthorizableAction> actions = Lists.newArrayList();
        for (AuthorizableActionProvider p : providers) {
            actions.addAll(p.getAuthorizableActions(securityProvider));
        }
        return actions;
    }
}