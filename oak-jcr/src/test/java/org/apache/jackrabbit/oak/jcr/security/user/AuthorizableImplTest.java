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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RangeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.value.StringValue;
import org.junit.Test;

/**
 * AuthorizableImplTest...
 */
public class AuthorizableImplTest extends AbstractUserTest {

    private List<String> protectedUserProps = new ArrayList<String>();
    private List<String> protectedGroupProps = new ArrayList<String>();

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        protectedUserProps.add(UserConstants.REP_PASSWORD);
        protectedUserProps.add(UserConstants.REP_IMPERSONATORS);
        protectedUserProps.add(UserConstants.REP_PRINCIPAL_NAME);

        protectedUserProps.add(UserConstants.REP_MEMBERS);
        protectedGroupProps.add(UserConstants.REP_PRINCIPAL_NAME);
    }

    private static void checkProtected(Property prop) throws RepositoryException {
        assertTrue(prop.getDefinition().isProtected());
    }

    @Test
    public void testRemoveAdmin() throws NotExecutableException {
        String adminID = superuser.getUserID();
        try {
            Authorizable admin = userMgr.getAuthorizable(adminID);
            if (admin == null) {
                throw new NotExecutableException("Admin user does not exist");
            }
            admin.remove();
            fail("The admin user cannot be removed.");
        } catch (RepositoryException e) {
            // OK superuser cannot be removed. not even by the superuser itself.
        }
    }

    @Test
    public void testSetSpecialProperties() throws NotExecutableException, RepositoryException {
        Value v = superuser.getValueFactory().createValue("any_value");

        for (String pName : protectedUserProps) {
            try {
                user.setProperty(pName, v);
                superuser.save();
                fail("changing the '" + pName + "' property on a User should fail.");
            } catch (RepositoryException e) {
                // success
            } finally {
                superuser.refresh(false);
            }
        }

        for (String pName : protectedGroupProps) {
            try {
                group.setProperty(pName, v);
                superuser.save();
                fail("changing the '" + pName + "' property on a Group should fail.");
            } catch (RepositoryException e) {
                // success
            } finally {
                superuser.refresh(false);
            }
        }
    }

    @Test
    public void testRemoveSpecialProperties() throws NotExecutableException, RepositoryException {
        for (String pName : protectedUserProps) {
            try {
                user.removeProperty(pName);
                superuser.save();
                fail("removing the '" + pName + "' property on a User should fail.");
            } catch (RepositoryException e) {
                // success
            } finally {
                superuser.refresh(false);
            }
        }
        for (String pName : protectedGroupProps) {
            try {
                group.removeProperty(pName);
                superuser.save();
                fail("removing the '" + pName + "' property on a Group should fail.");
            } catch (RepositoryException e) {
                // success
            } finally {
                superuser.refresh(false);
            }
        }
    }

    @Test
    public void testProtectedUserProperties() throws NotExecutableException, RepositoryException {
        UserImpl user = (UserImpl) getTestUser(superuser);
        Node n = user.getNode();

        if (n.hasProperty(UserConstants.REP_PASSWORD)) {
            checkProtected(n.getProperty(UserConstants.REP_PASSWORD));
        }
        if (n.hasProperty(UserConstants.REP_PRINCIPAL_NAME)) {
            checkProtected(n.getProperty(UserConstants.REP_PRINCIPAL_NAME));
        }
        if (n.hasProperty(UserConstants.REP_IMPERSONATORS)) {
           checkProtected(n.getProperty(UserConstants.REP_IMPERSONATORS));
        }
    }

    @Test
    public void testProtectedGroupProperties() throws NotExecutableException, RepositoryException {
        Node n = ((GroupImpl) group).getNode();

        if (n.hasProperty(UserConstants.REP_PRINCIPAL_NAME)) {
            checkProtected(n.getProperty(UserConstants.REP_PRINCIPAL_NAME));
        }
        if (n.hasProperty(UserConstants.REP_MEMBERS)) {
            checkProtected(n.getProperty(UserConstants.REP_MEMBERS));
        }
    }

    @Test
    public void testMembersPropertyType() throws NotExecutableException, RepositoryException {
        Node n = ((GroupImpl) group).getNode();

        if (!n.hasProperty(UserConstants.REP_MEMBERS)) {
            group.addMember(getTestUser(superuser));
        }

        Property p = n.getProperty(UserConstants.REP_MEMBERS);
        for (Value v : p.getValues()) {
            assertEquals(PropertyType.WEAKREFERENCE, v.getType());
        }
    }

    @Test
    public void testMemberOfRangeIterator() throws NotExecutableException, RepositoryException {
        Authorizable auth = null;
        Group group = null;

        try {
            auth = userMgr.createUser(getTestPrincipal().getName(), "pw");
            group = userMgr.createGroup(getTestPrincipal());
            superuser.save();

            Iterator<Group> groups = auth.declaredMemberOf();
            assertTrue(groups instanceof RangeIterator);
            assertEquals(0, ((RangeIterator) groups).getSize());
            groups = auth.memberOf();
            assertTrue(groups instanceof RangeIterator);
            assertEquals(0, ((RangeIterator) groups).getSize());

            group.addMember(auth);
            groups = auth.declaredMemberOf();
            assertTrue(groups instanceof RangeIterator);
            assertEquals(1, ((RangeIterator) groups).getSize());

            groups = auth.memberOf();
            assertTrue(groups instanceof RangeIterator);
            assertEquals(1, ((RangeIterator) groups).getSize());

        } finally {
            if (auth != null) {
                auth.remove();
            }
            if (group != null) {
                group.remove();
            }
            superuser.save();
        }
    }

    @Test
    public void testSetSpecialPropertiesDirectly() throws NotExecutableException, RepositoryException {
        AuthorizableImpl user = (AuthorizableImpl) getTestUser(superuser);
        Node n = user.getNode();
        try {
            String pName = user.getPrincipalName();
            n.setProperty(UserConstants.REP_PRINCIPAL_NAME, new StringValue("any-value"));

            // should have failed => change value back.
            n.setProperty(UserConstants.REP_PRINCIPAL_NAME, new StringValue(pName));
            fail("Attempt to change protected property rep:principalName should fail.");
        } catch (ConstraintViolationException e) {
            // ok.
        }

        try {
            String imperson = "anyimpersonator";
            n.setProperty(
                    UserConstants.REP_IMPERSONATORS,
                    new Value[] {new StringValue(imperson)},
                    PropertyType.STRING);
            fail("Attempt to change protected property rep:impersonators should fail.");
        } catch (ConstraintViolationException e) {
            // ok.
        }
    }

    @Test
    public void testRemoveSpecialUserPropertiesDirectly() throws RepositoryException, NotExecutableException {
        AuthorizableImpl g = (AuthorizableImpl) getTestUser(superuser);
        Node n = g.getNode();
        try {
            n.getProperty(UserConstants.REP_PASSWORD).remove();
            fail("Attempt to remove protected property rep:password should fail.");
        } catch (ConstraintViolationException e) {
            // ok.
        }
        try {
            if (n.hasProperty(UserConstants.REP_PRINCIPAL_NAME)) {
                n.getProperty(UserConstants.REP_PRINCIPAL_NAME).remove();
                fail("Attempt to remove protected property rep:principalName should fail.");
            }
        } catch (ConstraintViolationException e) {
            // ok.
        }
    }

    @Test
    public void testRemoveSpecialGroupPropertiesDirectly() throws RepositoryException, NotExecutableException {
        Node n = ((GroupImpl) group).getNode();
        try {
            if (n.hasProperty(UserConstants.REP_PRINCIPAL_NAME)) {
                n.getProperty(UserConstants.REP_PRINCIPAL_NAME).remove();
                fail("Attempt to remove protected property rep:principalName should fail.");
            }
        } catch (ConstraintViolationException e) {
            // ok.
        }
        try {
            if (n.hasProperty(UserConstants.REP_MEMBERS)) {
                n.getProperty(UserConstants.REP_MEMBERS).remove();
                fail("Attempt to remove protected property rep:members should fail.");
            }
        } catch (ConstraintViolationException e) {
            // ok.
        }
    }

    @Test
    public void testUserGetProperties() throws RepositoryException, NotExecutableException {
        AuthorizableImpl user = (AuthorizableImpl) getTestUser(superuser);
        Node n = user.getNode();

        for (PropertyIterator it = n.getProperties(); it.hasNext();) {
            Property p = it.nextProperty();
            if (p.getDefinition().isProtected()) {
                assertFalse(user.hasProperty(p.getName()));
                assertNull(user.getProperty(p.getName()));
            } else {
                // authorizable defined property
                assertTrue(user.hasProperty(p.getName()));
                assertNotNull(user.getProperty(p.getName()));
            }
        }
    }

    @Test
    public void testGroupGetProperties() throws RepositoryException, NotExecutableException {
        Node n = ((GroupImpl) group).getNode();

        for (PropertyIterator it = n.getProperties(); it.hasNext();) {
            Property prop = it.nextProperty();
            if (prop.getDefinition().isProtected()) {
                assertFalse(group.hasProperty(prop.getName()));
                assertNull(group.getProperty(prop.getName()));
            } else {
                // authorizable defined property
                assertTrue(group.hasProperty(prop.getName()));
                assertNotNull(group.getProperty(prop.getName()));
            }
        }
    }

    @Test
    public void testSingleToMultiValued() throws Exception {
        AuthorizableImpl user = (AuthorizableImpl) getTestUser(superuser);
        UserManager uMgr = getUserManager(superuser);
        try {
            Value v = superuser.getValueFactory().createValue("anyValue");
            user.setProperty("someProp", v);
            if (!uMgr.isAutoSave()) {
                superuser.save();
            }
            Value[] vs = new Value[] {v, v};
            user.setProperty("someProp", vs);
            if (!uMgr.isAutoSave()) {
                superuser.save();
            }
        } finally {
            if (user.removeProperty("someProp") && !uMgr.isAutoSave()) {
                superuser.save();
            }
        }
    }

    @Test
    public void testMultiValuedToSingle() throws Exception {
        AuthorizableImpl user = (AuthorizableImpl) getTestUser(superuser);
        UserManager uMgr = getUserManager(superuser);
        try {
            Value v = superuser.getValueFactory().createValue("anyValue");
            Value[] vs = new Value[] {v, v};
            user.setProperty("someProp", vs);
            if (!uMgr.isAutoSave()) {
                superuser.save();
            }
            user.setProperty("someProp", v);
            if (!uMgr.isAutoSave()) {
                superuser.save();
            }
        } finally {
            if (user.removeProperty("someProp") && !uMgr.isAutoSave()) {
                superuser.save();
            }
        }
    }

    @Test
    public void testObjectMethods() throws Exception {
        final AuthorizableImpl user = (AuthorizableImpl) getTestUser(superuser);
        AuthorizableImpl user2 = (AuthorizableImpl) getTestUser(superuser);

        assertEquals(user, user2);
        assertEquals(user.hashCode(), user2.hashCode());
        Set<Authorizable> s = new HashSet<Authorizable>();
        s.add(user);
        assertFalse(s.add(user2));

        Authorizable user3 = new Authorizable() {

            public String getID() throws RepositoryException {
                return user.getID();
            }

            public boolean isGroup() {
                return user.isGroup();
            }

            public Principal getPrincipal() throws RepositoryException {
                return user.getPrincipal();
            }

            public Iterator<Group> declaredMemberOf() throws RepositoryException {
                return user.declaredMemberOf();
            }

            public Iterator<Group> memberOf() throws RepositoryException {
                return user.memberOf();
            }

            public void remove() throws RepositoryException {
                user.remove();
            }

            public Iterator<String> getPropertyNames() throws RepositoryException {
                return user.getPropertyNames();
            }

            public Iterator<String> getPropertyNames(String relPath) throws RepositoryException {
                return user.getPropertyNames(relPath);
            }

            public boolean hasProperty(String name) throws RepositoryException {
                return user.hasProperty(name);
            }

            public void setProperty(String name, Value value) throws RepositoryException {
                user.setProperty(name, value);
            }

            public void setProperty(String name, Value[] values) throws RepositoryException {
                user.setProperty(name, values);
            }

            public Value[] getProperty(String name) throws RepositoryException {
                return user.getProperty(name);
            }

            public boolean removeProperty(String name) throws RepositoryException {
                return user.removeProperty(name);
            }

            public String getPath() throws UnsupportedRepositoryOperationException, RepositoryException {
                return user.getPath();
            }
        };

        assertFalse(user.equals(user3));
        assertTrue(s.add(user3));
    }

    @Test
    public void testGetPath() throws Exception {
        AuthorizableImpl user = (AuthorizableImpl) getTestUser(superuser);
        try {
            assertEquals(user.getNode().getPath(), user.getPath());
        } catch (UnsupportedRepositoryOperationException e) {
            // ok.
        }
    }
}