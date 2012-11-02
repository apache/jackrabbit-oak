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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

/**
 * ExternalLoginModuleImpl... TODO
 */
public class TestLoginModule extends ExternalLoginModule {

    static Set<ExternalGroup> externalGroups = new HashSet<ExternalGroup>();
    static {
        externalGroups.add(new TestGroup("a", new String[] {"aa", "aaa"}));
        externalGroups.add(new TestGroup("b", new String[] {"a"}));
        externalGroups.add(new TestGroup("c", new String[0]));
    }

    static Map<String,Object> props = new HashMap<String, Object>();
    static {
        props.put("name", "Test User");
        props.put("profile/name", "Public Name");
        props.put("profile/age", 72);
        props.put("./email", "test@testuser.com");
    }
    static ExternalUser externalUser = new TestUser("testUser", externalGroups, props);

    boolean success;

    @Override
    protected boolean loginSucceeded() {
        return success;
    }

    @Override
    protected ExternalUser getExternalUser() {
        return externalUser;
    }

    @Override
    public boolean login() throws LoginException {
        Credentials creds = getCredentials();
        if (creds instanceof SimpleCredentials) {
            if (externalUser.getId().equals(((SimpleCredentials) creds).getUserID())) {
                success = true;
                return success;
            } else {
                throw new LoginException("Invalid user");
            }
        } else {
            return false;
        }
    }

    //--------------------------------------------------------------------------

    private static class TestUser implements ExternalUser {

        private final String userId;
        private final Set<ExternalGroup> groups;
        private final Map props;

        private TestUser(String userId, Set<ExternalGroup> groups, Map<String, Object> props) {
            this.userId = userId;
            this.groups = groups;
            this.props = props;
        }

        @Override
        public String getId() {
            return userId;
        }

        @Override
        public String getPassword() {
            return null;
        }

        @Override
        public Principal getPrincipal() {
            return new Principal() {
                @Override
                public String getName() {
                    return userId;
                }
            };
        }

        @Override
        public String getPath() {
            return null;
        }

        @Override
        public Set<ExternalGroup> getGroups() {
            return groups;
        }

        @Override
        public Map<String, ?> getProperties() {
            return props;
        }
    }

    private static class TestGroup extends TestUser implements ExternalGroup {

        private final String[] groupNames;

        private TestGroup(String id, String[] groupNames) {
            super(id, Collections.<ExternalGroup>emptySet(), Collections.<String, Object>emptyMap());
            this.groupNames = groupNames;
        }

        @Override
        public Set<ExternalGroup> getGroups() {
            Set<ExternalGroup> groups = new HashSet<ExternalGroup>();
            for (String gn : groupNames) {
                groups.add(new TestGroup(gn, new String[0]));
            }
            return groups;
        }
    }
}