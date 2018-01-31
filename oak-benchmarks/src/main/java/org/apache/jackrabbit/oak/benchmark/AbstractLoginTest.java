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

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;

/**
 * AbstractLoginTest... TODO
 */
abstract class AbstractLoginTest extends AbstractTest {

    public final static int COUNT = 1000;
    public final static String USER = "user";
    public final static int DEFAULT_ITERATIONS = -1;
    public final static long NO_CACHE = -1;

    private String runAsUser;
    private boolean runWithToken;
    private int noIterations;
    private long expiration;

    public AbstractLoginTest() {
        this("admin", false, DEFAULT_ITERATIONS);
    }

    public AbstractLoginTest(String runAsUser, boolean runWithToken, int noIterations) {
        this(runAsUser, runWithToken, noIterations, NO_CACHE);
    }

    public AbstractLoginTest(String runAsUser, boolean runWithToken, int noIterations, long expiration) {
        super();
        this.runAsUser = runAsUser;
        this.runWithToken = runWithToken;
        this.noIterations = noIterations;
        this.expiration = expiration;
    }

    @Override
    public void setUp(Repository repository, Credentials credentials) throws Exception {
        super.setUp(repository, buildCredentials(repository, credentials));

    }

    @Override
    public void beforeSuite() throws Exception {
        Session s = loginAdministrative();
        try {
            AccessControlUtils.addAccessControlEntry(s, "/", EveryonePrincipal.getInstance(), new String[]{Privilege.JCR_READ}, true);
            if (USER.equals(runAsUser)) {
                ((JackrabbitSession) s).getUserManager().createUser(USER, USER);
            }
        } finally {
            s.save();
            s.logout();
        }
    }

    @Override
    public void afterSuite() throws Exception {
        Session s = loginAdministrative();
        try {
            Authorizable authorizable = ((JackrabbitSession) s).getUserManager().getAuthorizable(USER);
            if (authorizable != null) {
                authorizable.remove();
                s.save();
            }
        } finally {
            s.logout();
        }
    }

    protected boolean customConfigurationParameters() {
        return noIterations != -1 || expiration > 0;
    }

    protected ConfigurationParameters prepare(ConfigurationParameters conf) {
        return conf;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (customConfigurationParameters()) {
            if (fixture instanceof OakRepositoryFixture) {
                return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                    @Override
                    public Jcr customize(Oak oak) {
                        ConfigurationParameters conf;
                        ConfigurationParameters iterations = ConfigurationParameters.EMPTY;
                        if (noIterations != DEFAULT_ITERATIONS) {
                            iterations = ConfigurationParameters.of(UserConstants.PARAM_PASSWORD_HASH_ITERATIONS,
                                    noIterations);
                        }
                        ConfigurationParameters cache = ConfigurationParameters.EMPTY;
                        if (expiration > 0) {
                            cache = ConfigurationParameters.of("cacheExpiration", expiration);
                        }

                        if (runWithToken) {
                            conf = ConfigurationParameters.of(
                                    TokenConfiguration.NAME, iterations,
                                    UserConfiguration.NAME, cache);
                        } else {
                            conf = ConfigurationParameters.of(
                                    UserConfiguration.NAME, ConfigurationParameters.of(iterations, cache));
                        }
                        conf = prepare(conf);
                        SecurityProvider sp = new SecurityProviderBuilder().with(conf).build();
                        return new Jcr(oak).with(sp);
                    }
                });
            } else {
                throw new UnsupportedOperationException("Not yet supported -> Change repository.xml to configure no of iterations.");
            }
        }
        return super.createRepository(fixture);
    }

    private Credentials buildCredentials(Repository repository, Credentials credentials) throws RepositoryException {
        Credentials creds;
        if ("admin".equals(runAsUser)) {
            creds = credentials;
        } else if ("anonymous".equals(runAsUser)) {
            creds = new GuestCredentials();
        } else {
            creds = new SimpleCredentials(USER, USER.toCharArray());
        }
        if (runWithToken) {
            Configuration.setConfiguration(ConfigurationUtil.getJackrabbit2Configuration(ConfigurationParameters.EMPTY));
            if (creds instanceof SimpleCredentials) {
                SimpleCredentials sc = (SimpleCredentials) creds;
                sc.setAttribute(".token", "");
                repository.login(sc).logout();
                creds = new TokenCredentials(sc.getAttribute(".token").toString());
            } else {
                throw new UnsupportedOperationException();
            }
        }
        return creds;
    }
}
