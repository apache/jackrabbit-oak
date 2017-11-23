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
package org.apache.jackrabbit.oak.security.user;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.ConstraintViolationException;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuthorizablePropertiesImplTest extends AbstractSecurityTest {

    private AuthorizablePropertiesImpl emptyProperties;

    private Authorizable user2;
    private AuthorizablePropertiesImpl properties;

    private ValueFactory vf;

    @Override
    public void before() throws Exception {
        super.before();

        User user = getTestUser();
        emptyProperties = new AuthorizablePropertiesImpl((AuthorizableImpl) user, getNamePathMapper());

        String id2 = "user2" + UUID.randomUUID().toString();
        user2 = getUserManager(root).createUser(id2, null, new PrincipalImpl(id2), PathUtils.getAncestorPath(user.getPath(), 1));

        vf = getValueFactory(root);
        Value v = vf.createValue("value");
        Value[] vArr =  new Value[] {vf.createValue(2), vf.createValue(30)};

        user2.setProperty("prop", v);
        user2.setProperty("mvProp", vArr);

        user2.setProperty("relPath/prop", v);
        user2.setProperty("relPath/mvProp", vArr);
        root.commit();

        properties = new AuthorizablePropertiesImpl((AuthorizableImpl) user2, getNamePathMapper());
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            user2.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    //-----------------------------------------------------------< getNames >---

    @Test(expected = RepositoryException.class)
    public void testGetNamesNullPath() throws Exception {
        emptyProperties.getNames("");
    }

    @Test(expected = RepositoryException.class)
    public void testGetNamesEmptyPath() throws Exception {
        emptyProperties.getNames("");
    }

    @Test(expected = RepositoryException.class)
    public void testGetNamesAbsPath() throws Exception {
        emptyProperties.getNames("/");
    }

    @Test(expected = RepositoryException.class)
    public void testGetNamesOutSideScope() throws Exception {
        emptyProperties.getNames("../..");
    }

    @Test(expected = RepositoryException.class)
    public void testGetNamesOtherUser() throws Exception {
        emptyProperties.getNames("../" + PathUtils.getName(user2.getPath()));
    }

    @Test(expected = RepositoryException.class)
    public void testGetNamesMissingResolutionToOakPath() throws Exception {
        AuthorizableProperties props = new AuthorizablePropertiesImpl((AuthorizableImpl) user2, new NamePathMapper.Default() {
            @Override
            public String getOakNameOrNull(@Nonnull String jcrName) {
                return null;
            }

            @Override
            public String getOakPath(String jcrPath) {
                return null;
            }
        });
        props.getNames("relPath");
    }

    @Test
    public void testGetNamesCurrent() throws Exception {
        assertFalse(emptyProperties.getNames(".").hasNext());
    }

    @Test
    public void testGetNamesCurrent2() throws Exception {
        Iterator<String> names = properties.getNames(".");

        Set<String> expected = ImmutableSet.of("prop", "mvProp");
        assertEquals(expected, ImmutableSet.copyOf(names));
    }

    @Test(expected = RepositoryException.class)
    public void testGetNamesNonExistingRelPath() throws Exception {
        properties.getNames("nonExisting");
    }

    @Test
    public void testGetNamesRelPath() throws Exception {
        Iterator<String> names = properties.getNames("relPath");

        Set<String> expected = ImmutableSet.of("prop", "mvProp");
        assertEquals(expected, ImmutableSet.copyOf(names));
    }


    //--------------------------------------------------------< setProperty >---

    @Test(expected = ConstraintViolationException.class)
    public void testSetAuthorizableId() throws Exception {
        properties.setProperty(UserConstants.REP_AUTHORIZABLE_ID, vf.createValue("otherId"));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSetPrimaryType() throws Exception {
        properties.setProperty("relPath/" + JcrConstants.JCR_PRIMARYTYPE, vf.createValue(NodeTypeConstants.NT_OAK_UNSTRUCTURED, PropertyType.NAME));
    }

    @Test
    public void testSetNewProperty() throws Exception {
        properties.setProperty("prop2", vf.createValue(true));
        assertTrue(properties.hasProperty("prop2"));
    }

    @Test
    public void testSetNewPropertyWithRelPath() throws Exception {
        properties.setProperty("relPath/prop2", vf.createValue("val"));
        assertTrue(properties.hasProperty("relPath/prop2"));
    }

    @Test
    public void testSetNewPropertyNewRelPath() throws Exception {
        properties.setProperty("newPath/prop2", vf.createValue("val"));
        assertTrue(properties.hasProperty("newPath/prop2"));
    }

    @Test
    public void testSetModifyProperty() throws Exception {
        Value v = vf.createValue("newValue");
        properties.setProperty("prop", v);
        assertArrayEquals(new Value[] {v}, properties.getProperty("prop"));
    }

    @Test
    public void testSetPropertyChangeMvStatus() throws Exception {
        Value v = vf.createValue("newValue");
        properties.setProperty("mvProp", v);
        assertArrayEquals(new Value[]{v}, properties.getProperty("mvProp"));
    }

    @Test
    public void testSetPropertyChangeMvStatus2() throws Exception {
        Value v = vf.createValue("newValue");
        properties.setProperty("relPath/prop", new Value[] {v, v});
        assertArrayEquals(new Value[]{v, v}, properties.getProperty("relPath/prop"));
    }

    @Test(expected = RepositoryException.class)
    public void testSetMissingResolutionToOakPath() throws Exception {
        AuthorizableProperties props = new AuthorizablePropertiesImpl((AuthorizableImpl) user2, new NamePathMapper.Default() {
            @Override
            public String getOakNameOrNull(@Nonnull String jcrName) {
                return null;
            }

            @Override
            public String getOakPath(String jcrPath) {
                return null;
            }
        });
        props.setProperty("relPath/prop", vf.createValue("value"));
    }

    @Test
    public void testSetPropertyNullArray() throws Exception  {
        properties.setProperty("mvProp", (Value[]) null);
        assertFalse(properties.hasProperty("mvProp"));

        properties.setProperty("relPath/prop", (Value[]) null);
        assertFalse(properties.hasProperty("relPath/prop"));
    }

    @Test
    public void testSetPropertyNull() throws Exception  {
        properties.setProperty("mvProp", (Value) null);
        assertFalse(properties.hasProperty("mvProp"));

        properties.setProperty("relPath/prop", (Value) null);
        assertFalse(properties.hasProperty("relPath/prop"));
    }

    @Test(expected = RepositoryException.class)
    public void testSetOutSideScope() throws Exception {
        properties.setProperty("../../prop", vf.createValue("newValue"));
    }

    @Test(expected = RepositoryException.class)
    public void testSetPropertyOtherUser() throws Exception {
        emptyProperties.setProperty("../" + PathUtils.getName(user2.getPath()) + "/prop", vf.createValue("newValue"));
    }

    //-----------------------------------------------------< removeProperty >---

    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAuthorizableId() throws Exception {
        properties.removeProperty(UserConstants.REP_AUTHORIZABLE_ID);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testRemovePrimaryType() throws Exception {
        properties.removeProperty("relPath/" + JcrConstants.JCR_PRIMARYTYPE);
    }

    @Test
    public void testRemoveNonExisting() throws Exception {
        assertFalse(properties.removeProperty("nonExisting"));
        assertFalse(properties.removeProperty("relPath/nonExisting"));
        assertFalse(emptyProperties.removeProperty("prop"));
    }

    @Test
    public void testRemove() throws Exception {
        assertTrue(properties.removeProperty("mvProp"));
        assertTrue(properties.removeProperty("relPath/prop"));
    }

    @Test(expected = RepositoryException.class)
    public void testRemoveMissingResolutionToOakPath() throws Exception {
        AuthorizableProperties props = new AuthorizablePropertiesImpl((AuthorizableImpl) user2, new NamePathMapper.Default() {
            @Override
            public String getOakNameOrNull(@Nonnull String jcrName) {
                return null;
            }

            @Override
            public String getOakPath(String jcrPath) {
                return null;
            }
        });
        props.removeProperty("relPath/prop");
    }

    @Test(expected = RepositoryException.class)
    public void testRemovePropertyOutSideScope() throws Exception {
        properties.removeProperty("../../" + JcrConstants.JCR_PRIMARYTYPE);
    }

    @Test(expected = RepositoryException.class)
    public void testRemoveNonExistingPropertyOutSideScope() throws Exception {
        properties.removeProperty("../../nonExistingTree/nonExistingProp");
    }

    @Test(expected = RepositoryException.class)
    public void testRemovePropertyOtherUser() throws Exception {
        emptyProperties.removeProperty("../" + PathUtils.getName(user2.getPath()) + "/prop");
    }
}