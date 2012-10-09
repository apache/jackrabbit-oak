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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.util.Text;
import org.apache.jackrabbit.value.StringValue;
import org.junit.Test;

/**
 * AuthorizableTest...
 */
public class AuthorizableTest extends AbstractUserTest {

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
    public void testSetProperty() throws NotExecutableException, RepositoryException {
        Authorizable auth = getTestUser(superuser);

        String propName = "Fullname";
        Value v = superuser.getValueFactory().createValue("Super User");
        try {
            auth.setProperty(propName, v);
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        try {
            boolean found = false;
            for (Iterator<String> it = auth.getPropertyNames(); it.hasNext() && !found;) {
                found = propName.equals(it.next());
            }
            assertTrue(found);

            found = false;
            for (Iterator<String> it = auth.getPropertyNames("."); it.hasNext() && !found;) {
                found = propName.equals(it.next());
            }
            assertTrue(found);

            assertTrue(auth.hasProperty(propName));
            assertTrue(auth.hasProperty("./" + propName));
            
            assertTrue(auth.getProperty(propName).length == 1);

            assertEquals(v, auth.getProperty(propName)[0]);
            assertEquals(v, auth.getProperty("./" + propName)[0]);

            assertTrue(auth.removeProperty(propName));
            assertFalse(auth.hasProperty(propName));
            
            superuser.save();
        } finally {
            // try to remove the property again even if previous calls failed.
            auth.removeProperty(propName);
            superuser.save();
        }
    }

    @Test
    public void testSetMultiValueProperty() throws NotExecutableException, RepositoryException {
        Authorizable auth = getTestUser(superuser);

        String propName = "Fullname";
        Value[] v = new Value[] {superuser.getValueFactory().createValue("Super User")};
        try {
            auth.setProperty(propName, v);
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        try {
            boolean found = false;
            for (Iterator<String> it = auth.getPropertyNames(); it.hasNext() && !found;) {
                found = propName.equals(it.next());
            }
            assertTrue(found);

            found = false;
            for (Iterator<String> it = auth.getPropertyNames("."); it.hasNext() && !found;) {
                found = propName.equals(it.next());
            }
            assertTrue(found);
            
            assertTrue(auth.hasProperty(propName));
            assertTrue(auth.hasProperty("./" + propName));
            
            assertEquals(Arrays.asList(v), Arrays.asList(auth.getProperty(propName)));
            assertEquals(Arrays.asList(v), Arrays.asList(auth.getProperty("./" + propName)));

            assertTrue(auth.removeProperty(propName));
            assertFalse(auth.hasProperty(propName));
            
            superuser.save();
        } finally {
            // try to remove the property again even if previous calls failed.
            auth.removeProperty(propName);
            superuser.save();
        }
    }

    @Test
    public void testSetPropertyByRelPath() throws NotExecutableException, RepositoryException {
        Authorizable auth = getTestUser(superuser);
        Value[] v = new Value[] {superuser.getValueFactory().createValue("Super User")};

        List<String> relPaths = new ArrayList<String>();
        relPaths.add("testing/Fullname");
        relPaths.add("testing/Email");
        relPaths.add("testing/testing/testing/Fullname");
        relPaths.add("testing/testing/testing/Email");

        for (String relPath : relPaths) {
            try {
                auth.setProperty(relPath, v);
                superuser.save();

                assertTrue(auth.hasProperty(relPath));
                String propName = Text.getName(relPath);
                assertFalse(auth.hasProperty(propName));
            } finally {
                // try to remove the property even if previous calls failed.
                auth.removeProperty(relPath);
                superuser.save();
            }
        }
    }

    @Test
    public void testSetPropertyInvalidRelativePath() throws NotExecutableException, RepositoryException {
        Authorizable auth = getTestUser(superuser);
        Value[] v = new Value[] {superuser.getValueFactory().createValue("Super User")};

        List<String> invalidPaths = new ArrayList<String>();
        // try setting outside of tree defined by the user.
        invalidPaths.add("../testing/Fullname");
        invalidPaths.add("../../testing/Fullname");
        invalidPaths.add("testing/testing/../../../Fullname");
        // try absolute path -> must fail
        invalidPaths.add("/testing/Fullname");

        for (String invalidRelPath : invalidPaths) {
            try {
                auth.setProperty(invalidRelPath, v);
                fail("Modifications outside of the scope of the authorizable must fail. Path was: " + invalidRelPath);
            } catch (Exception e) {
                // success.
            } finally {
                superuser.refresh(false);
            }
        }
    }

    @Test
    public void testGetPropertyByInvalidRelativePath() throws NotExecutableException, RepositoryException {
        Authorizable auth = getTestUser(superuser);

        List<String> wrongPaths = new ArrayList<String>();
        wrongPaths.add("../jcr:primaryType");
        wrongPaths.add("../../jcr:primaryType");
        wrongPaths.add("../testing/jcr:primaryType");
        for (String path : wrongPaths) {
            assertNull(auth.getProperty(path));
        }

        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("/testing/jcr:primaryType");
        invalidPaths.add("..");
        invalidPaths.add(".");
        invalidPaths.add(null);
        for (String invalidPath : invalidPaths) {
            try {
                assertNull(auth.getProperty(invalidPath));
            } catch (Exception e) {
                // success
            }
        }
    }

    @Test
    public void testHasPropertyByInvalidRelativePath() throws NotExecutableException, RepositoryException {
        Authorizable auth = getTestUser(superuser);

        List<String> wrongPaths = new ArrayList<String>();
        wrongPaths.add("../jcr:primaryType");
        wrongPaths.add("../../jcr:primaryType");
        wrongPaths.add("../testing/jcr:primaryType");
        for (String path : wrongPaths) {
            assertFalse(auth.hasProperty(path));
        }


        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("..");
        invalidPaths.add(".");
        invalidPaths.add(null);

        for (String invalidPath : invalidPaths) {
            try {
                assertFalse(auth.hasProperty(invalidPath));
            } catch (Exception e) {
                // success
            }
        }
    }

    @Test
    public void testGetPropertyNames() throws NotExecutableException, RepositoryException {
        Authorizable auth = getTestUser(superuser);

        String propName = "Fullname";
        Value v = superuser.getValueFactory().createValue("Super User");
        try {
            auth.setProperty(propName, v);
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        try {
            for (Iterator<String> it = auth.getPropertyNames(); it.hasNext();) {
                String name = it.next();
                assertTrue(auth.hasProperty(name));
                assertNotNull(auth.getProperty(name));
            }
        } finally {
            // try to remove the property again even if previous calls failed.
            auth.removeProperty(propName);
            superuser.save();
        }
    }

    @Test
    public void testGetPropertyNamesByRelPath() throws NotExecutableException, RepositoryException {
        Authorizable auth = getTestUser(superuser);

        String relPath = "testing/Fullname";
        Value v = superuser.getValueFactory().createValue("Super User");
        try {
            auth.setProperty(relPath, v);
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        try {
            for (Iterator<String> it = auth.getPropertyNames(); it.hasNext();) {
                String name = it.next();
                assertFalse("Fullname".equals(name));
            }

            for (Iterator<String> it = auth.getPropertyNames("testing"); it.hasNext();) {
                String name = it.next();
                String rp = "testing/" + name;
                
                assertFalse(auth.hasProperty(name));
                assertNull(auth.getProperty(name));

                assertTrue(auth.hasProperty(rp));
                assertNotNull(auth.getProperty(rp));
            }
            for (Iterator<String> it = auth.getPropertyNames("./testing"); it.hasNext();) {
                String name = it.next();
                String rp = "testing/" + name;

                assertFalse(auth.hasProperty(name));
                assertNull(auth.getProperty(name));

                assertTrue(auth.hasProperty(rp));
                assertNotNull(auth.getProperty(rp));
            }
        } finally {
            // try to remove the property again even if previous calls failed.
            auth.removeProperty(relPath);
            superuser.save();
        }
    }

    @Test
    public void testGetPropertyNamesByInvalidRelPath() throws NotExecutableException, RepositoryException {
        Authorizable auth = getTestUser(superuser);

        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("../");
        invalidPaths.add("../../");
        invalidPaths.add("../testing");
        invalidPaths.add("/testing");
        invalidPaths.add(null);

        for (String invalidRelPath : invalidPaths) {
            try {
                auth.getPropertyNames(invalidRelPath);
                fail("Calling Authorizable#getPropertyNames with " + invalidRelPath + " must fail.");
            } catch (Exception e) {
                // success
            }
        }
    }

    @Test
    public void testGetNotExistingProperty() throws RepositoryException, NotExecutableException {
        Authorizable auth = getTestUser(superuser);
        String hint = "Fullname";
        String propName = hint;
        int i = 0;
        while (auth.hasProperty(propName)) {
            propName = hint + i;
            i++;
        }
        assertNull(auth.getProperty(propName));
        assertFalse(auth.hasProperty(propName));
    }

    @Test
    public void testRemoveNotExistingProperty() throws RepositoryException, NotExecutableException {
        Authorizable auth = getTestUser(superuser);
        String hint = "Fullname";
        String propName = hint;
        int i = 0;
        while (auth.hasProperty(propName)) {
            propName = hint + i;
            i++;
        }
        assertFalse(auth.removeProperty(propName));
        superuser.save();
    }

    /**
     * Removing an authorizable that is still listed as member of a group.
     * @throws javax.jcr.RepositoryException
     * @throws org.apache.jackrabbit.test.NotExecutableException
     */
    public void testRemoveListedAuthorizable() throws RepositoryException, NotExecutableException {
        String newUserId = null;
        Group newGroup = null;

        try {
            Principal uP = getTestPrincipal();
            User newUser = userMgr.createUser(uP.getName(), uP.getName());
            superuser.save();
            newUserId = newUser.getID();

            newGroup = userMgr.createGroup(getTestPrincipal());
            newGroup.addMember(newUser);
            superuser.save();

            // remove the new user that is still listed as member.
            newUser.remove();
            superuser.save();
        } finally {
            if (newUserId != null) {
                Authorizable u = userMgr.getAuthorizable(newUserId);
                if (u != null) {
                    if (newGroup != null) {
                        newGroup.removeMember(u);
                    }
                    u.remove();
                }
            }
            if (newGroup != null) {
                newGroup.remove();
            }
            superuser.save();
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
        User user = getTestUser(superuser);
        Node n = getNode(user, superuser);

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
        Node n = getNode(group, superuser);

        if (n.hasProperty(UserConstants.REP_PRINCIPAL_NAME)) {
            checkProtected(n.getProperty(UserConstants.REP_PRINCIPAL_NAME));
        }
        if (n.hasProperty(UserConstants.REP_MEMBERS)) {
            checkProtected(n.getProperty(UserConstants.REP_MEMBERS));
        }
    }

    @Test
    public void testMembersPropertyType() throws NotExecutableException, RepositoryException {
        Node n = getNode(group, superuser);

        if (!n.hasProperty(UserConstants.REP_MEMBERS)) {
            group.addMember(getTestUser(superuser));
        }

        Property p = n.getProperty(UserConstants.REP_MEMBERS);
        for (Value v : p.getValues()) {
            assertEquals(PropertyType.WEAKREFERENCE, v.getType());
        }
    }

    @Test
    public void testSetSpecialPropertiesDirectly() throws NotExecutableException, RepositoryException {
        Authorizable user = getTestUser(superuser);
        Node n = getNode(user, superuser);
        try {
            String pName = user.getPrincipal().getName();
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
        Authorizable g = getTestUser(superuser);
        Node n = getNode(g, superuser);
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
        Node n = getNode(group, superuser);
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
        Authorizable user = getTestUser(superuser);
        Node n = getNode(user, superuser);

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
        Node n = getNode(group, superuser);

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
        Authorizable user = getTestUser(superuser);
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
        Authorizable user = getTestUser(superuser);
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
        final Authorizable user = getTestUser(superuser);
        Authorizable user2 = getTestUser(superuser);

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
}