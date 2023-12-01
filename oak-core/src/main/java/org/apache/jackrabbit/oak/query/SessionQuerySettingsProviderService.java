/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.query;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.query.SessionQuerySettings;
import org.apache.jackrabbit.oak.spi.query.SessionQuerySettingsProvider;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Overrides oak.fastQuerySize system property when available.
 */
@Component(configurationPolicy = ConfigurationPolicy.REQUIRE, immediate = true)
@Designate(ocd = SessionQuerySettingsProviderService.Configuration.class)
public class SessionQuerySettingsProviderService implements SessionQuerySettingsProvider {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Session Query Settings Provider Service",
            description = "Provides Session-specific query settings exposed by Oak QueryEngine."
    )
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Configuration {
        @AttributeDefinition(name = "Direct Counts Principals", description = "Principal names for which executed query result counts directly reflect the index estimate.")
        String[] directCountsPrincipals() default {};
    }

    void configure(Configuration config) {
        this.directCountsAllowedPrincipals = Optional.ofNullable(config)
                .map(cfg -> (Set<String>) new HashSet<>(Arrays.asList(cfg.directCountsPrincipals())))
                .orElse(Collections.emptySet());
    }

    @Activate
    protected void activate(Configuration config) {
        configure(config);
    }

    @Modified
    protected void modified(Configuration config) {
        configure(config);
    }

    private Set<String> directCountsAllowedPrincipals = Collections.emptySet();

    @Override
    public SessionQuerySettings getQuerySettings(@NotNull ContentSession session) {
        final Set<String> principals = directCountsAllowedPrincipals;
        final boolean directCountsAllowed = session.getAuthInfo().getPrincipals().stream()
                .anyMatch(principal -> principals.contains(principal.getName()));
        return new SessionQuerySettings() {
            @Override
            public boolean useDirectResultCount() {
                return directCountsAllowed;
            }
        };
    }
}
