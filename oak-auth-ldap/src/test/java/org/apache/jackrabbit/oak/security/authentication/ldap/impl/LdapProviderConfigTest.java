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
package org.apache.jackrabbit.oak.security.authentication.ldap.impl;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class LdapProviderConfigTest {

    @Test
    public void testOfEmptyConfigurationParameters() {
        LdapProviderConfig config = LdapProviderConfig.of(ConfigurationParameters.EMPTY);
        assertEquals(LdapProviderConfig.PARAM_NAME_DEFAULT, config.getName());
    }

    @Test
    public void testOfConfigurationParameters() {
        LdapProviderConfig config = LdapProviderConfig.of(ConfigurationParameters.of(LdapProviderConfig.PARAM_NAME, "name"));
        assertEquals("name", config.getName());
    }

    @Test
    public void testOfConfigurationParametersIncludingSearchTimeout() {
        LdapProviderConfig config = LdapProviderConfig.of(ConfigurationParameters.of(LdapProviderConfig.PARAM_SEARCH_TIMEOUT, 25));
        assertEquals(25, config.getSearchTimeout());
    }

    @Test
    public void testOfAllConfigurationParameters() {

        Map<String, Object> params = new HashMap<>();

        String testName = "testname";
        String testLdapHost = "testhost.org";
        int testLdapPort = LdapProviderConfig.PARAM_LDAP_PORT_DEFAULT + 1;
        boolean testUseSsl = !LdapProviderConfig.PARAM_USE_SSL_DEFAULT;
        boolean testUseTls = !LdapProviderConfig.PARAM_USE_TLS_DEFAULT;
        boolean testNoCertCheck = !LdapProviderConfig.PARAM_NO_CERT_CHECK_DEFAULT;
        String testBindDn = "cn=testBindDn";
        String testBindPassword = "testPwd";
        String testSearchTimeout = "1d 1h 1m 1s 1ms";
        long testSearchTimeoutMs = 1 + 1000 * (1 + 60 * (1 + 60 * (1 + 24)));
        boolean testUseUidForExtId = !LdapProviderConfig.PARAM_USE_UID_FOR_EXT_ID_DEFAULT;
        String[] testCustomAttributes = new String[] {"a","b","c"};
        String testGroupMemberAttribute = "testMemberAttr";

        boolean testAdminPoolLookupOnValidate = !LdapProviderConfig.PARAM_ADMIN_POOL_LOOKUP_ON_VALIDATE_DEFAULT;
        int testAdminPoolMaxActive = LdapProviderConfig.PARAM_ADMIN_POOL_MAX_ACTIVE_DEFAULT + 1;
        String testAdminPoolMinEvictableIdleTime = "2d 2h 2m 2s 2ms";
        long testAdminPoolMinEvictableIdleTimeMs = 2 * (1 + 1000 * (1 + 60 * (1 + 60 * (1 + 24))));
        String testAdminPoolTimeBetweenEvictionRuns = "3d 3h 3m 3s 3ms";
        long testAdminPoolTimeBetweenEvictionRunsMs = 3 * (1 + 1000 * (1 + 60 * (1 + 60 * (1 + 24))));
        int testAdminPoolNumTestsPerEvictionRun = LdapProviderConfig.PARAM_ADMIN_POOL_NUM_TESTS_PER_EVICTION_RUN_DEFAULT + 1;

        boolean testUserPoolLookupOnValidate = !LdapProviderConfig.PARAM_USER_POOL_LOOKUP_ON_VALIDATE_DEFAULT;
        int testUserPoolMaxActive = LdapProviderConfig.PARAM_USER_POOL_MAX_ACTIVE_DEFAULT + 2;
        String testUserPoolMinEvictableIdleTime = "4d 4h 4m 4s 4ms";
        long testUserPoolMinEvictableIdleTimeMs = 4 * (1 + 1000 * (1 + 60 * (1 + 60 * (1 + 24))));
        String testUserPoolTimeBetweenEvictionRuns = "5d 5h 5m 5s 5ms";
        long testUserPoolTimeBetweenEvictionRunsMs = 5 * (1 + 1000 * (1 + 60 * (1 + 60 * (1 + 24))));
        int testUserPoolNumTestsPerEvictionRun = LdapProviderConfig.PARAM_USER_POOL_NUM_TESTS_PER_EVICTION_RUN_DEFAULT + 2;

        String testUserBaseDn = "ou=people,dc=org";
        String[] testUserObjectClass = new String[] {"inetOrgPerson"};
        String testUserIdAttribute = "foo";
        String testUserExtraFilter = "(cn=*)";
        boolean testUserMakeDnPath = !LdapProviderConfig.PARAM_USER_MAKE_DN_PATH_DEFAULT;

        String testGroupBaseDn = "ou=groups,dc=org";
        String[] testGroupObjectClass = new String[] {"posixGroup"};
        String testGroupNameAttribute = "bar";
        String testGroupExtraFilter = "(ou=*)";
        boolean testGroupMakeDnPath = !LdapProviderConfig.PARAM_GROUP_MAKE_DN_PATH_DEFAULT;

        params.put(LdapProviderConfig.PARAM_NAME, testName);
        params.put(LdapProviderConfig.PARAM_LDAP_HOST, testLdapHost);
        params.put(LdapProviderConfig.PARAM_LDAP_PORT, testLdapPort);
        params.put(LdapProviderConfig.PARAM_USE_SSL, testUseSsl);
        params.put(LdapProviderConfig.PARAM_USE_TLS, testUseTls);
        params.put(LdapProviderConfig.PARAM_NO_CERT_CHECK, testNoCertCheck);
        params.put(LdapProviderConfig.PARAM_BIND_DN, testBindDn);
        params.put(LdapProviderConfig.PARAM_BIND_PASSWORD, testBindPassword);
        params.put(LdapProviderConfig.PARAM_SEARCH_TIMEOUT, testSearchTimeout);
        params.put(LdapProviderConfig.PARAM_USE_UID_FOR_EXT_ID, testUseUidForExtId);
        params.put(LdapProviderConfig.PARAM_CUSTOM_ATTRIBUTES, testCustomAttributes);
        params.put(LdapProviderConfig.PARAM_GROUP_MEMBER_ATTRIBUTE, testGroupMemberAttribute);

        params.put(LdapProviderConfig.PARAM_ADMIN_POOL_LOOKUP_ON_VALIDATE, testAdminPoolLookupOnValidate);
        params.put(LdapProviderConfig.PARAM_ADMIN_POOL_MAX_ACTIVE, testAdminPoolMaxActive);
        params.put(LdapProviderConfig.PARAM_ADMIN_POOL_MIN_EVICTABLE_IDLE_TIME, testAdminPoolMinEvictableIdleTime);
        params.put(LdapProviderConfig.PARAM_ADMIN_POOL_TIME_BETWEEN_EVICTION_RUNS, testAdminPoolTimeBetweenEvictionRuns);
        params.put(LdapProviderConfig.PARAM_ADMIN_POOL_NUM_TESTS_PER_EVICTION_RUN, testAdminPoolNumTestsPerEvictionRun);

        params.put(LdapProviderConfig.PARAM_USER_POOL_LOOKUP_ON_VALIDATE, testUserPoolLookupOnValidate);
        params.put(LdapProviderConfig.PARAM_USER_POOL_MAX_ACTIVE, testUserPoolMaxActive);
        params.put(LdapProviderConfig.PARAM_USER_POOL_MIN_EVICTABLE_IDLE_TIME, testUserPoolMinEvictableIdleTime);
        params.put(LdapProviderConfig.PARAM_USER_POOL_TIME_BETWEEN_EVICTION_RUNS, testUserPoolTimeBetweenEvictionRuns);
        params.put(LdapProviderConfig.PARAM_USER_POOL_NUM_TESTS_PER_EVICTION_RUN, testUserPoolNumTestsPerEvictionRun);

        params.put(LdapProviderConfig.PARAM_USER_BASE_DN, testUserBaseDn);
        params.put(LdapProviderConfig.PARAM_USER_OBJECTCLASS, testUserObjectClass);
        params.put(LdapProviderConfig.PARAM_USER_ID_ATTRIBUTE, testUserIdAttribute);
        params.put(LdapProviderConfig.PARAM_USER_EXTRA_FILTER, testUserExtraFilter);
        params.put(LdapProviderConfig.PARAM_USER_MAKE_DN_PATH, testUserMakeDnPath);

        params.put(LdapProviderConfig.PARAM_GROUP_BASE_DN, testGroupBaseDn);
        params.put(LdapProviderConfig.PARAM_GROUP_OBJECTCLASS, testGroupObjectClass);
        params.put(LdapProviderConfig.PARAM_GROUP_NAME_ATTRIBUTE, testGroupNameAttribute);
        params.put(LdapProviderConfig.PARAM_GROUP_EXTRA_FILTER, testGroupExtraFilter);
        params.put(LdapProviderConfig.PARAM_GROUP_MAKE_DN_PATH, testGroupMakeDnPath);

        LdapProviderConfig config = LdapProviderConfig.of(ConfigurationParameters.of(params));
        assertEquals(testName, config.getName());
        assertEquals(testLdapHost, config.getHostname());
        assertEquals(testLdapPort, config.getPort());
        assertEquals(testUseSsl, config.useSSL());
        assertEquals(testUseTls, config.useTLS());
        assertEquals(testNoCertCheck, config.noCertCheck());
        assertEquals(testBindDn, config.getBindDN());
        assertEquals(testBindPassword, config.getBindPassword());
        assertEquals(testSearchTimeoutMs, config.getSearchTimeout());
        assertEquals(testUseUidForExtId, config.getUseUidForExtId());
        assertArrayEquals(testCustomAttributes, config.getCustomAttributes());
        assertEquals(testGroupMemberAttribute, config.getGroupMemberAttribute());

        LdapProviderConfig.PoolConfig adminPoolConfig = config.getAdminPoolConfig();
        assertEquals(testAdminPoolLookupOnValidate, adminPoolConfig.lookupOnValidate());
        assertEquals(testAdminPoolMaxActive, adminPoolConfig.getMaxActive());
        assertEquals(testAdminPoolMinEvictableIdleTimeMs, adminPoolConfig.getMinEvictableIdleTimeMillis());
        assertEquals(testAdminPoolTimeBetweenEvictionRunsMs, adminPoolConfig.getTimeBetweenEvictionRunsMillis());
        assertEquals(testAdminPoolNumTestsPerEvictionRun, adminPoolConfig.getNumTestsPerEvictionRun());

        LdapProviderConfig.PoolConfig userPoolConfig = config.getUserPoolConfig();
        assertEquals(testUserPoolLookupOnValidate, userPoolConfig.lookupOnValidate());
        assertEquals(testUserPoolMaxActive, userPoolConfig.getMaxActive());
        assertEquals(testUserPoolMinEvictableIdleTimeMs, userPoolConfig.getMinEvictableIdleTimeMillis());
        assertEquals(testUserPoolTimeBetweenEvictionRunsMs, userPoolConfig.getTimeBetweenEvictionRunsMillis());
        assertEquals(testUserPoolNumTestsPerEvictionRun, userPoolConfig.getNumTestsPerEvictionRun());

        LdapProviderConfig.Identity userConfig = config.getUserConfig();
        assertEquals(testUserBaseDn, userConfig.getBaseDN());
        assertArrayEquals(testUserObjectClass, userConfig.getObjectClasses());
        assertEquals(testUserIdAttribute, userConfig.getIdAttribute());
        assertEquals(testUserExtraFilter, userConfig.getExtraFilter());
        assertEquals(testUserMakeDnPath, userConfig.makeDnPath());

        LdapProviderConfig.Identity groupConfig = config.getGroupConfig();
        assertEquals(testGroupBaseDn, groupConfig.getBaseDN());
        assertArrayEquals(testGroupObjectClass, groupConfig.getObjectClasses());
        assertEquals(testGroupNameAttribute, groupConfig.getIdAttribute());
        assertEquals(testGroupExtraFilter, groupConfig.getExtraFilter());
        assertEquals(testGroupMakeDnPath, groupConfig.makeDnPath());
    }

    @Test
    public void testGetMemberOfSearchFilter() {
        Map<String, Object> params = new HashMap<>();
        LdapProviderConfig config = LdapProviderConfig.of(ConfigurationParameters.of(params));
        assertEquals("(&(" + LdapProviderConfig.PARAM_GROUP_MEMBER_ATTRIBUTE_DEFAULT + "=cn=bar)(objectclass=" + LdapProviderConfig.PARAM_GROUP_OBJECTCLASS_DEFAULT[0] + "))",
                config.getMemberOfSearchFilter("cn=bar"));
        params.put(LdapProviderConfig.PARAM_GROUP_MEMBER_ATTRIBUTE, "foo");
        config = LdapProviderConfig.of(ConfigurationParameters.of(params));
        assertEquals("(&(foo=cn=bar)(objectclass=" + LdapProviderConfig.PARAM_GROUP_OBJECTCLASS_DEFAULT[0] + "))",
                config.getMemberOfSearchFilter("cn=bar"));
        params.put(LdapProviderConfig.PARAM_GROUP_OBJECTCLASS, new String[] {"posixGroup"});
        config = LdapProviderConfig.of(ConfigurationParameters.of(params));
        assertEquals("(&(foo=cn=bar)(objectclass=posixGroup))",
                config.getMemberOfSearchFilter("cn=bar"));
        params.put(LdapProviderConfig.PARAM_GROUP_OBJECTCLASS, new String[] {"posixGroup", "groupOfUniqueNames"});
        config = LdapProviderConfig.of(ConfigurationParameters.of(params));
        assertEquals("(&(foo=cn=bar)(objectclass=posixGroup)(objectclass=groupOfUniqueNames))",
                config.getMemberOfSearchFilter("cn=bar"));
    }

    @Test
    public void testEncodeFilterValueNormal() {
        //test a value that doesn't need escaping (see RFC4515 chapter 3)
        StringBuilder builder = new StringBuilder();
        for (int k = 1; k <= 0x27; k++) {
            builder.append((char) k);
        }
        for (int k = 0x2b; k <= 0x5b; k++) {
            builder.append((char) k);
        }
        for (int k = 0x5d; k <= 0x7f; k++) {
            builder.append((char) k);
        }
        builder.append(Character.toChars(0x80));
        builder.append(Character.toChars(0x7ff));
        builder.append(Character.toChars(0x800));
        builder.append(Character.toChars(0xffff));
        builder.append(Character.toChars(0x10000));
        builder.append(Character.toChars(0x10ffff));
        String value = builder.toString();
        assertEquals(value, LdapProviderConfig.encodeFilterValue(value));
    }

    @Test
    public void testEncodeFilterValueEscaped() {
        //test the encoding of character that need escaping (see RFC4515 chapter 3)
        String value = "\u0000*()\\";
        String encodedValue = "\\00\\2A\\28\\29\\5C";
        assertEquals(encodedValue, LdapProviderConfig.encodeFilterValue(value));
    }
}