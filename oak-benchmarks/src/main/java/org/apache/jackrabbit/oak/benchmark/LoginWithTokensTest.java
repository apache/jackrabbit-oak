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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.util.Text;

/**
 * Measure impact of synchronous token cleanup on the repository login with
 * tokens over multiple users. Concurrency can be set via the benchmark runner.
 *
 * Default expiration time login tokens is 2 hours, this benchmark uses 15
 * seconds to allow for cleanup during the benchmark.
 *
 */
public class LoginWithTokensTest extends AbstractLoginTest {

    private static final String REL_TEST_PATH = "testPath";
    private static final String USER = "user";
    private final Random r = new Random();

    // defaults to 10k
    private final int numberOfUsers;

    private final long tknExpy = TimeUnit.SECONDS.toMillis(15);

    private final long cleanupThreshold = 100;

    public LoginWithTokensTest(int numberOfUsers) {
        super("admin", true, DEFAULT_ITERATIONS);
        this.numberOfUsers = numberOfUsers;
    }

    @Override
    protected boolean customConfigurationParameters() {
        return true;
    }

    @Override
    protected ConfigurationParameters prepare(ConfigurationParameters conf) {
        ConfigurationParameters tkns = ConfigurationParameters.of(TokenProvider.PARAM_TOKEN_EXPIRATION, tknExpy,
                "tokenCleanupThreshold", cleanupThreshold);

        ConfigurationParameters tokenConfig = ConfigurationParameters.of(TokenConfiguration.NAME, tkns);
        return ConfigurationParameters.of(conf, tokenConfig);
    }

    @Override
    public void beforeSuite() throws Exception {
        super.beforeSuite();

        Session s = loginAdministrative();
        try {
            UserManager userManager = ((JackrabbitSession) s).getUserManager();

            for (int i = 0; i < numberOfUsers; i++) {
                String id = USER + i;
                userManager.createUser(id, id, new PrincipalImpl(id), REL_TEST_PATH);
            }

        } finally {
            s.save();
            s.logout();
        }
        System.out.println("setup done, created " + numberOfUsers + " users.");
    }

    @Override
    public void afterSuite() throws Exception {
        Session s = loginAdministrative();
        try {
            Authorizable authorizable = ((JackrabbitSession) s).getUserManager().getAuthorizable(USER + "0");
            if (authorizable != null) {
                Node n = s.getNode(Text.getRelativeParent(authorizable.getPath(), 1));
                n.remove();
            }

            s.save();
        } finally {
            s.logout();
        }
    }

    @Override
    public void runTest() throws RepositoryException {
        Repository repository = getRepository();
        String t = USER + r.nextInt(numberOfUsers);
        SimpleCredentials creds = new SimpleCredentials(t, t.toCharArray());
        creds.setAttribute(".token", "");
        repository.login(creds).logout();
    }

}
