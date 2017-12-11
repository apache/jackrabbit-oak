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
package org.apache.jackrabbit.oak.benchmark;

import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.Repository;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Test the effect of multiple authorization configurations on the general read
 * operations.
 *
 * TODO: setup configured number of cugs.
 */
public class CugTest extends ReadDeepTreeTest {

    private final ConfigurationParameters params;
    private final boolean reverseOrder;

    protected CugTest(boolean runAsAdmin, int itemsToRead, boolean singleSession, @Nonnull List<String> supportedPaths, boolean reverseOrder) {
        super(runAsAdmin, itemsToRead, false, singleSession);
        this.params = ConfigurationParameters.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(
                    "cugSupportedPaths", supportedPaths.toArray(new String[supportedPaths.size()]),
                    "cugEnabled", true));
        this.reverseOrder = reverseOrder;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    return new Jcr(oak).with(createSecurityProvider());
                }
            });
        } else {
            throw new IllegalArgumentException("Fixture " + fixture + " not supported for this benchmark.");
        }
    }

    @Override
    protected String getImportFileName() {
        return "deepTree_everyone.xml";
    }

    @Override
    protected String getTestNodeName() {
        return "CugTest";
    }

    protected SecurityProvider createSecurityProvider() {
        return newTestSecurityProvider(params, reverseOrder);
    }

    private static SecurityProvider newTestSecurityProvider(@Nonnull ConfigurationParameters params,
            boolean reverseOrder) {
        SecurityProvider delegate = new SecurityProviderBuilder().with(params).build();
        CompositeAuthorizationConfiguration authorizationConfiguration = (CompositeAuthorizationConfiguration) delegate
                .getConfiguration((AuthorizationConfiguration.class));
        AuthorizationConfiguration defaultAuthorization = checkNotNull(authorizationConfiguration.getDefaultConfig());
        if (reverseOrder) {
            authorizationConfiguration.addConfiguration(defaultAuthorization);
            authorizationConfiguration.addConfiguration(new CugConfiguration(delegate));
        } else {
            authorizationConfiguration.addConfiguration(new CugConfiguration(delegate));
            authorizationConfiguration.addConfiguration(defaultAuthorization);
        }
        return delegate;
    }
}