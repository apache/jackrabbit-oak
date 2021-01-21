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
package org.apache.jackrabbit.oak.benchmark.authentication.external;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;

import javax.jcr.LoginException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL;
import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT;
import static org.junit.Assert.assertEquals;

/**
 * Login against the {@link ExternalLoginModule} with a randomly selected user.
 * The first login of a given user will trigger the user-synchronization mechanism.
 * Subsequent login calls will only result in an extra sync-call if the configured
 * expiration time is reached.
 *
 * Configuration options as defined in {@link AbstractExternalTest}.
 */
public class ExternalLoginTest extends AbstractExternalTest {

    private final int numberOfUsers;
    private final int numberOfGroups;
    private final Reporter reporter;
    StatisticsProvider statisticsProvider;
    private final List<String> auto;

    private Set<String> uniques;
    private AtomicLong err;

    public ExternalLoginTest(int numberOfUsers, int numberOfGroups, long expTime, boolean dynamicMembership,
            @NotNull List<String> autoMembership, boolean report, @NotNull StatisticsProvider statsProvider) {
        super(numberOfUsers, numberOfGroups, expTime, dynamicMembership, autoMembership);
        this.numberOfUsers = numberOfUsers;
        this.numberOfGroups = numberOfGroups;
        this.reporter = new Reporter(report);
        this.statisticsProvider = statsProvider;
        this.auto = autoMembership;
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();
        reporter.beforeSuite();
        uniques = Collections.synchronizedSet(new HashSet<>(numberOfUsers));
        err = new AtomicLong();
        AuthenticationConfiguration authenticationConfiguration = getSecurityProvider()
                .getConfiguration(AuthenticationConfiguration.class);
        authenticationConfiguration.getMonitors(statisticsProvider);
    }

    @Override
    protected void afterSuite() throws Exception {
        reporter.afterSuite();
        System.out.println("Unique users " + uniques.size() + " out of total " + numberOfUsers + ". Groups "
                + numberOfGroups + ". Err " + err.get() + ". Seed " + seed);
        super.afterSuite();
    }

    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();
        reporter.beforeTest();
    }

    @Override
    protected void afterTest() throws Exception {
        super.afterTest();
        reporter.afterTest();
    }

    @Override
    protected void runTest() throws Exception {
        String id = getRandomUserId();
        Session s = null;
        try {
            s = getRepository().login(new SimpleCredentials(id, new char[0]));
            Set<String> principals = extractGroupPrincipals(s, id);
            Set<String> expected = new TreeSet<>(StreamSupport.stream(idp.getDeclaredGroupRefs(id).spliterator(), false)
                    .map(ExternalIdentityRef::getId).collect(Collectors.toSet()));
            expected.addAll(auto);
            assertEquals(expected, principals);
        } catch (LoginException ex) {
            // ignore, will be reflected in the jmx stats
            err.incrementAndGet();
        } finally {
            if (s != null) {
                s.logout();
            }
            uniques.add(id);
        }
    }

    private static Set<String> extractGroupPrincipals(Session s, String userId) throws Exception {
        SessionDelegate sd = (SessionDelegate) FieldUtils.readField(s, "sd", true);
        Set<String> principals = new TreeSet<>(sd.getAuthInfo().getPrincipals().stream()
                .map((p) -> p.getName().startsWith("p_") ? p.getName().substring(2) : p.getName())
                .collect(Collectors.toSet()));
        principals.remove("everyone");
        principals.remove(userId);
        return principals;
    }

    protected Configuration createConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(
                                GuestLoginModule.class.getName(),
                                OPTIONAL,
                                ImmutableMap.of()),
                        new AppConfigurationEntry(
                                TokenLoginModule.class.getName(),
                                SUFFICIENT,
                                ImmutableMap.of()),
                        new AppConfigurationEntry(
                                ExternalLoginModule.class.getName(),
                                SUFFICIENT,
                                ImmutableMap.of(
                                        ExternalLoginModule.PARAM_SYNC_HANDLER_NAME, syncConfig.getName(),
                                        ExternalLoginModule.PARAM_IDP_NAME, idp.getName())),
                        new AppConfigurationEntry(
                                LoginModuleImpl.class.getName(),
                                SUFFICIENT,
                                ImmutableMap.of())
                };
            }
        };
    }

    private static class Reporter {
        private final long LIMIT = Long.getLong("flushAt", 1000);

        private final boolean doReport;

        private long count;
        private long start;

        public Reporter(boolean doReport) {
            this.doReport = doReport;
        }

        public void afterTest() {
            if (!doReport) {
                return;
            }
            count++;
            report(false);
        }

        private void report(boolean end) {
            if (end || count % LIMIT == 0) {
                long dur = System.currentTimeMillis() - start;
                System.out.println(dur + " ms, " + count + " tests");
                start = System.currentTimeMillis();
                count = 0;
            }
        }

        public void beforeTest() {
            if (!doReport) {
                return;
            }
        }

        public void beforeSuite() {
            if (!doReport) {
                return;
            }
            System.out.println("Reporting enabled.");
            start = System.currentTimeMillis();
            count = 0;
        }

        public void afterSuite() {
            if (!doReport) {
                return;
            }
            report(true);
        }
    }
}
