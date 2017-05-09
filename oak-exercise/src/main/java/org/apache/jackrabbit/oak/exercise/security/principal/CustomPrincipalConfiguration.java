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
package org.apache.jackrabbit.oak.exercise.security.principal;

import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom principal configuration that is disabled by default.
 */
@Component(metatype = true, policy = ConfigurationPolicy.REQUIRE)
@Service({PrincipalConfiguration.class, org.apache.jackrabbit.oak.spi.security.SecurityConfiguration.class})
public class CustomPrincipalConfiguration extends ConfigurationBase implements PrincipalConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CustomPrincipalConfiguration.class);

    // EXERCISE define sensible properties (e.g. configuration parameters for principal lookup on a third party system)
    @Property(name = "knownPrincipals", value = {}, cardinality = 100)
    private String[] knownPrincipals = new String[0];

    @Nonnull
    @Override
    public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
        log.info("CustomPrincipalConfiguration.getPrincipalManager");
        return new PrincipalManagerImpl(getPrincipalProvider(root, namePathMapper));
    }

    @Nonnull
    @Override
    public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
        log.info("CustomPrincipalConfiguration.getPrincipalProvider");
        return new CustomPrincipalProvider(knownPrincipals);
    }

    @Nonnull
    @Override
    public String getName() {
        return PrincipalConfiguration.NAME;
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(Map<String, Object> properties) {
        knownPrincipals = PropertiesUtil.toStringArray(properties.get("knownPrincipals"), new String[0]);
        log.info("CustomPrincipalConfiguration.activate: " + knownPrincipals);
    }

    @SuppressWarnings("UnusedDeclaration")
    @Modified
    private void modified(Map<String, Object> properties) {
        knownPrincipals = PropertiesUtil.toStringArray(properties.get("knownPrincipals"), new String[0]);
        log.info("CustomPrincipalConfiguration.modified: " + knownPrincipals);
    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    private void deactivate(Map<String, Object> properties) {
        knownPrincipals = new String[0];
        log.info("CustomPrincipalConfiguration.deactivate");
    }
}