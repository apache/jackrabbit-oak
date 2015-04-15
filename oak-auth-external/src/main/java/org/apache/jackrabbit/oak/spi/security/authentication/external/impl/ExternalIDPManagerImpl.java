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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.service.component.ComponentContext;

/**
 * {@code ExternalIDPManagerImpl} is used to manage registered external identity provider. This class automatically
 * tracks the IDPs that are registered via OSGi but can also be used in non-OSGi environments by manually adding and
 * removing the providers.
 */
@Component(immediate = true)
@Service
public class ExternalIDPManagerImpl extends AbstractServiceTracker<ExternalIdentityProvider> implements ExternalIdentityProviderManager {

    /**
     * Default constructor used by OSGi
     */
    @SuppressWarnings("UnusedDeclaration")
    public ExternalIDPManagerImpl() {
        super(ExternalIdentityProvider.class);
    }

    /**
     * Constructor used by non OSGi
     * @param whiteboard the whiteboard
     */
    public ExternalIDPManagerImpl(Whiteboard whiteboard) {
        super(ExternalIdentityProvider.class);
        start(whiteboard);
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(ComponentContext ctx) {
        start(new OsgiWhiteboard(ctx.getBundleContext()));
    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    private void deactivate() {
        stop();
    }

    @Override
    public ExternalIdentityProvider getProvider(@Nonnull String name) {
        for (ExternalIdentityProvider provider: getServices()) {
            if (name.equals(provider.getName())) {
                return provider;
            }
        }
        return null;
    }
}