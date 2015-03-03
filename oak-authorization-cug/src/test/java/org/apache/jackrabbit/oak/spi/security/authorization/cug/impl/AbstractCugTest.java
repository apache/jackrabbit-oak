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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.util.NodeUtil;

/**
 * Base class for CUG related test that setup the authorization configuration
 * to expose the CUG specific implementations of {@code AccessControlManager}
 * and {@code PermissionProvider}.
 */
public class AbstractCugTest extends AbstractSecurityTest implements CugConstants {

    static final String SUPPORTED_PATH = "/content";
    static final String SUPPORTED_PATH2 = "/content2";
    static final String UNSUPPORTED_PATH = "/testNode";
    static final String INVALID_PATH = "/path/to/non/existing/tree";

    static final ConfigurationParameters CUG_CONFIG = ConfigurationParameters.of(
            CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[] {SUPPORTED_PATH, SUPPORTED_PATH2},
            CugConstants.PARAM_CUG_ENABLED, true);

    @Override
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        NodeUtil content = rootNode.addChild("content", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        content.addChild("subtree", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        rootNode.addChild("content2", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        rootNode.addChild("testNode", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            root.getTree(SUPPORTED_PATH).remove();
            root.getTree(UNSUPPORTED_PATH).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = new CugSecurityProvider(super.getSecurityProvider());
        }
        return securityProvider;
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(ImmutableMap.of(
                AuthorizationConfiguration.NAME, CUG_CONFIG)
        );
    }

    void createCug(@Nonnull String absPath, @Nonnull Principal principal) throws RepositoryException {
        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies(absPath);
        while (it.hasNext()) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            if (policy instanceof CugPolicy) {
                ((CugPolicy) policy).addPrincipals(principal);
                acMgr.setPolicy(absPath, policy);
                return;
            }
        }
        throw new IllegalStateException("Unable to create CUG at " + absPath);
    }

    final class CugSecurityProvider implements SecurityProvider {

        private final SecurityProvider base;

        private final CugConfiguration cugConfiguration;

        private CugSecurityProvider(@Nonnull SecurityProvider base) {
            this.base = base;
            cugConfiguration = new CugConfiguration(this);
        }

        @Nonnull
        @Override
        public ConfigurationParameters getParameters(@Nullable String name) {
            return base.getParameters(name);
        }

        @Nonnull
        @Override
        public Iterable<? extends SecurityConfiguration> getConfigurations() {
            Set<SecurityConfiguration> configs = (Set<SecurityConfiguration>) base.getConfigurations();

            CompositeAuthorizationConfiguration composite = new CompositeAuthorizationConfiguration(this);
            Iterator<SecurityConfiguration> it = configs.iterator();
            while (it.hasNext()) {
                SecurityConfiguration sc = it.next();
                if (sc instanceof AuthorizationConfiguration) {
                    composite.addConfiguration((AuthorizationConfiguration) sc);
                    it.remove();
                }
            }
            composite.addConfiguration(cugConfiguration);
            configs.add(composite);

            return configs;
        }

        @Nonnull
        @Override
        public <T> T getConfiguration(@Nonnull Class<T> configClass) {
            T c = base.getConfiguration(configClass);
            if (AuthorizationConfiguration.class == configClass) {
                CompositeAuthorizationConfiguration composite = new CompositeAuthorizationConfiguration(this);
                composite.addConfiguration(cugConfiguration);
                composite.addConfiguration((AuthorizationConfiguration) c);
                return (T) composite;
            } else {
                return c;
            }
        }
    }
}