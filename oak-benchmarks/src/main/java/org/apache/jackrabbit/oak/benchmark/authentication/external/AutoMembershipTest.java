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

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DynamicSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;

import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL;
import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT;

public class AutoMembershipTest extends AbstractExternalTest {
    
    private final boolean dynamicSync;
    private final int numberOfUsers;

    public AutoMembershipTest(int numberOfUsers, int numberOfGroups, boolean dynamicMembership, @NotNull List<String> autoMembership) {
        super(numberOfUsers, numberOfGroups, Long.MAX_VALUE, dynamicMembership, autoMembership);
        this.dynamicSync = dynamicMembership;
        this.numberOfUsers = numberOfUsers;
    }

    @Override
    protected void expandSyncConfig() {
        super.expandSyncConfig();
        syncConfig.group().setDynamicGroups(syncConfig.user().getDynamicMembership());
    }
    
    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();

        Session systemSession = null;
        try {
            systemSession = systemLogin();
            UserManager userManager = ((JackrabbitSession) systemSession).getUserManager();
            ValueFactory valueFactory = systemSession.getValueFactory();

            Set<String> memberIds = new HashSet<>();
            SyncContext ctx = (dynamicSync) ? new DynamicSyncContext(syncConfig, idp, userManager, valueFactory) :
                    new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);
            for (int i = 0; i < numberOfUsers; i++) {
                String uid = "u" + i;
                ctx.sync(idp.getUser(uid));
                memberIds.add(uid);
            }

            // create a test group and extra nested groups
            Group testGroup = userManager.createGroup("testGroup");
            for (int i = 0; i < 10; i++) {
                Group nested = userManager.createGroup("nested_"+i);
                memberIds.add("nested_"+i);
            }
            // add all members to the test group
            testGroup.addMembers(memberIds.toArray(new String[0]));
            systemSession.save();
        } finally {
            if (systemSession != null) {
                systemSession.logout();
            }
        }
    }

    @Override
    protected void runTest() throws Exception {
        String id = getRandomUserId();
        Session systemSession = null;
        try {
            systemSession = systemLogin();
            UserManager um = ((JackrabbitSession) systemSession).getUserManager();
            
            Group g = um.getAuthorizable("testGroup", Group.class);
            Authorizable member = um.getAuthorizable(id);
            // trigger auto-membership evaluation
            g.isMember(member);
        } finally {
            if (systemSession != null) {
                systemSession.logout();
            }
        }

    }

    @Override
    protected Configuration createConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(
                                GuestLoginModule.class.getName(),
                                OPTIONAL,
                                Map.of()),
                        new AppConfigurationEntry(
                                TokenLoginModule.class.getName(),
                                SUFFICIENT,
                                Map.of()),
                        new AppConfigurationEntry(
                                ExternalLoginModule.class.getName(),
                                SUFFICIENT,
                                Map.of(
                                        ExternalLoginModule.PARAM_SYNC_HANDLER_NAME, syncConfig.getName(),
                                        ExternalLoginModule.PARAM_IDP_NAME, idp.getName())),
                        new AppConfigurationEntry(
                                LoginModuleImpl.class.getName(),
                                SUFFICIENT,
                                Map.of())
                };
            }
        };
    }

    @Override
    protected ConfigurationParameters getSecurityConfiguration() {
        return ConfigurationParameters.of(UserConfiguration.NAME, ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT));
    }
}