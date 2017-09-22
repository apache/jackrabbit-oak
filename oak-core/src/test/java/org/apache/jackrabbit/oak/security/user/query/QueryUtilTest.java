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
package org.apache.jackrabbit.oak.security.user.query;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.oak.commons.QueryUtils;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class QueryUtilTest {

    private static void assertSearchRoot(@Nonnull Map<AuthorizableType, String> mapping, @Nonnull ConfigurationParameters params) {
        for (AuthorizableType type : mapping.keySet()) {
            String expected = QueryConstants.SEARCH_ROOT_PATH + mapping.get(type);
            assertEquals(expected, QueryUtil.getSearchRoot(type, params));
        }
    }

    @Test
    public void testGetSearchRootDefault() {
        Map<AuthorizableType, String> defaultPaths = ImmutableMap.of(
                AuthorizableType.USER, UserConstants.DEFAULT_USER_PATH,
                AuthorizableType.GROUP, UserConstants.DEFAULT_GROUP_PATH,
                AuthorizableType.AUTHORIZABLE, "/rep:security/rep:authorizables");

        assertSearchRoot(defaultPaths, ConfigurationParameters.EMPTY);
    }

    @Test
    public void testGetSearchRootSingleConfiguredPath() {
        String path = "/configured/user_and_group/path";

        for (AuthorizableType type : AuthorizableType.values()) {
            assertEquals(QueryConstants.SEARCH_ROOT_PATH + path, QueryUtil.getSearchRoot(type, ConfigurationParameters.of(UserConstants.PARAM_USER_PATH, path, UserConstants.PARAM_GROUP_PATH, path)));
        }
    }

    @Test
    public void testGetSearchRootUserPathParentOfGroup() {
        ConfigurationParameters params = ConfigurationParameters.of(
                UserConstants.PARAM_USER_PATH, "/configured/users",
                UserConstants.PARAM_GROUP_PATH, "/configured/users/groups");

        Map<AuthorizableType, String> paths = ImmutableMap.of(
                AuthorizableType.USER, "/configured/users",
                AuthorizableType.GROUP, "/configured/users/groups",
                AuthorizableType.AUTHORIZABLE, "/configured/users");

        assertSearchRoot(paths, params);
    }

    @Test
    public void testGetSearchRootGroupPathParentOfUser() {
        ConfigurationParameters params = ConfigurationParameters.of(
                UserConstants.PARAM_USER_PATH, "/configured/groups/users",
                UserConstants.PARAM_GROUP_PATH, "/configured/groups");

        Map<AuthorizableType, String> paths = ImmutableMap.of(
                AuthorizableType.USER, "/configured/groups/users",
                AuthorizableType.GROUP, "/configured/groups",
                AuthorizableType.AUTHORIZABLE, "/configured/groups");

        assertSearchRoot(paths, params);
    }

    @Test
    public void testGetSearchRootNoCommonAncestor() {
        ConfigurationParameters params = ConfigurationParameters.of(
                UserConstants.PARAM_USER_PATH, "/users",
                UserConstants.PARAM_GROUP_PATH, "/groups");

        Map<AuthorizableType, String> paths = ImmutableMap.of(
                AuthorizableType.USER, "/users",
                AuthorizableType.GROUP, "/groups",
                AuthorizableType.AUTHORIZABLE, "/");

        assertSearchRoot(paths, params);
    }

    @Test
    public void testGetSearchRoot() {
        ConfigurationParameters params = ConfigurationParameters.of(
                UserConstants.PARAM_USER_PATH, "/configured/user/path",
                UserConstants.PARAM_GROUP_PATH, "/configured/group/path");

        Map<AuthorizableType, String> paths = ImmutableMap.of(
                AuthorizableType.USER, "/configured/user/path",
                AuthorizableType.GROUP, "/configured/group/path",
                AuthorizableType.AUTHORIZABLE, "/configured");

        assertSearchRoot(paths, params);
    }

    @Test
    public void testNodeTypeName() {
        Map<AuthorizableType, String> ntNames = ImmutableMap.of(
                AuthorizableType.USER, UserConstants.NT_REP_USER,
                AuthorizableType.GROUP, UserConstants.NT_REP_GROUP,
                AuthorizableType.AUTHORIZABLE, UserConstants.NT_REP_AUTHORIZABLE);

        for (AuthorizableType type : ntNames.keySet()) {
            assertEquals(ntNames.get(type), QueryUtil.getNodeTypeName(type));
        }
    }

    @Test
    public void testEscapeNodeName() {
        List<String> names = ImmutableList.of("name", JcrConstants.JCR_CREATED, "%name", "a%name", "name%");
        for (String name : names) {
            assertEquals(QueryUtils.escapeNodeName(name), QueryUtil.escapeNodeName(name));
        }
    }

    @Test
    public void testFormatString() throws Exception {
        String value = "'string\\value";
        assertEquals("'"+QueryUtils.escapeForQuery(value)+"'", QueryUtil.format(ValueFactoryImpl.createValue(PropertyValues.newString(value), NamePathMapper.DEFAULT)));
    }

    @Test
    public void testFormatBoolean() throws Exception {
        assertEquals("'"+Boolean.TRUE.toString()+"'", QueryUtil.format(ValueFactoryImpl.createValue(PropertyValues.newBoolean(true), NamePathMapper.DEFAULT)));

    }

    @Test
    public void testFormatLong() throws Exception {
        Value longV = ValueFactoryImpl.createValue(PropertyValues.newLong(Long.MAX_VALUE), NamePathMapper.DEFAULT);
        assertEquals(String.valueOf(Long.MAX_VALUE), QueryUtil.format(longV));
    }

    @Test
    public void testFormatDouble() throws Exception {
        Value doubleV = ValueFactoryImpl.createValue(PropertyValues.newDouble(Double.valueOf(2.3)), NamePathMapper.DEFAULT);
        assertEquals(String.valueOf(2.3), QueryUtil.format(doubleV));
    }

    @Test
    public void testFormatDate() throws Exception {
        Value dateV = ValueFactoryImpl.createValue(PropertyValues.newDate("dateString"), NamePathMapper.DEFAULT);
        assertEquals("xs:dateTime('dateString')", QueryUtil.format(dateV));
    }

    @Test(expected = RepositoryException.class)
    public void testFormatOtherTypes() throws Exception {
        Value nameValue = ValueFactoryImpl.createValue(PropertyValues.newName(JcrConstants.JCR_CREATED), NamePathMapper.DEFAULT);
        QueryUtil.format(nameValue);
    }

    @Test
    public void testEscapeForQuery() throws Exception {
        NamePathMapper namePathMapper = new NamePathMapperImpl(new LocalNameMapper(
                ImmutableMap.of(NamespaceRegistry.PREFIX_JCR, NamespaceRegistry.NAMESPACE_JCR),
                ImmutableMap.of("myPrefix", NamespaceRegistry.NAMESPACE_JCR)));

        String value = "'string\\value";
        assertEquals(QueryUtils.escapeForQuery("myPrefix:"+value), QueryUtil.escapeForQuery("jcr:"+value, namePathMapper));
    }

    @Test
    public void testGetCollation() throws Exception {
        assertSame(RelationOp.LT, QueryUtil.getCollation(QueryBuilder.Direction.DESCENDING));
        assertSame(RelationOp.GT, QueryUtil.getCollation(QueryBuilder.Direction.ASCENDING));
    }
}