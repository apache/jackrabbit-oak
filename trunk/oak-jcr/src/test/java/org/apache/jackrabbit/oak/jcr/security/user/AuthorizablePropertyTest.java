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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.util.Text;
import org.apache.jackrabbit.value.StringValue;
import org.junit.Test;

/**
 * Tests for user property specific API.
 */
public class AuthorizablePropertyTest extends AbstractUserTest {

    private Map<String, Boolean> protectedUserProps = new HashMap<String, Boolean>();
    private Map<String, Boolean> protectedGroupProps = new HashMap<String, Boolean>();

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        protectedUserProps.put(UserConstants.REP_PASSWORD, false);
        protectedUserProps.put(UserConstants.REP_IMPERSONATORS, true);
        protectedUserProps.put(UserConstants.REP_PRINCIPAL_NAME, false);

        protectedGroupProps.put(UserConstants.REP_MEMBERS, true);
        protectedGroupProps.put(UserConstants.REP_PRINCIPAL_NAME, false);
    }

    private static void checkProtected(Property prop) throws RepositoryException {
        assertTrue(prop.getDefinition().isProtected());
    }

    @Test
    public void testSetProperty() throws NotExecutableException, RepositoryException {
        String propName = "Fullname";
        Value v = superuser.getValueFactory().createValue("Super User");
        try {
            user.setProperty(propName, v);
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        try {
            boolean found = false;
            for (Iterator<String> it = user.getPropertyNames(); it.hasNext() && !found;) {
                found = propName.equals(it.next());
            }
            assertTrue(found);

            found = false;
            for (Iterator<String> it = user.getPropertyNames("."); it.hasNext() && !found;) {
                found = propName.equals(it.next());
            }
            assertTrue(found);

            assertTrue(user.hasProperty(propName));
            assertTrue(user.hasProperty("./" + propName));

            assertTrue(user.getProperty(propName).length == 1);

            assertEquals(v, user.getProperty(propName)[0]);
            assertEquals(v, user.getProperty("./" + propName)[0]);

            assertTrue(user.removeProperty(propName));
            assertFalse(user.hasProperty(propName));

            superuser.save();
        } finally {
            // try to remove the property again even if previous calls failed.
            user.removeProperty(propName);
            superuser.save();
        }
    }

    @Test
    public void testSetMultiValueProperty() throws NotExecutableException, RepositoryException {
        String propName = "Fullname";
        Value[] v = new Value[] {superuser.getValueFactory().createValue("Super User")};
        try {
            user.setProperty(propName, v);
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        try {
            boolean found = false;
            for (Iterator<String> it = user.getPropertyNames(); it.hasNext() && !found;) {
                found = propName.equals(it.next());
            }
            assertTrue(found);

            found = false;
            for (Iterator<String> it = user.getPropertyNames("."); it.hasNext() && !found;) {
                found = propName.equals(it.next());
            }
            assertTrue(found);

            assertTrue(user.hasProperty(propName));
            assertTrue(user.hasProperty("./" + propName));

            assertEquals(Arrays.asList(v), Arrays.asList(user.getProperty(propName)));
            assertEquals(Arrays.asList(v), Arrays.asList(user.getProperty("./" + propName)));

            assertTrue(user.removeProperty(propName));
            assertFalse(user.hasProperty(propName));

            superuser.save();
        } finally {
            // try to remove the property again even if previous calls failed.
            user.removeProperty(propName);
            superuser.save();
        }
    }

    @Test
    public void testSetPropertyByRelPath() throws NotExecutableException, RepositoryException {
        Value[] v = new Value[] {superuser.getValueFactory().createValue("Super User")};

        List<String> relPaths = new ArrayList<String>();
        relPaths.add("testing/Fullname");
        relPaths.add("testing/Email");
        relPaths.add("testing/testing/testing/Fullname");
        relPaths.add("testing/testing/testing/Email");

        for (String relPath : relPaths) {
            try {
                user.setProperty(relPath, v);
                superuser.save();

                assertTrue(user.hasProperty(relPath));
                String propName = Text.getName(relPath);
                assertFalse(user.hasProperty(propName));
            } finally {
                // try to remove the property even if previous calls failed.
                user.removeProperty(relPath);
                superuser.save();
            }
        }
    }

    @Test
    public void testSetPropertyInvalidRelativePath() throws NotExecutableException, RepositoryException {
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
                user.setProperty(invalidRelPath, v);
                fail("Modifications outside of the scope of the authorizable must fail. Path was: " + invalidRelPath);
            } catch (RepositoryException e) {
                // success.
            } finally {
                superuser.refresh(false);
            }
        }
    }

    @Test
    public void testGetPropertyByInvalidRelativePath() throws NotExecutableException, RepositoryException {
        List<String> wrongPaths = new ArrayList<String>();
        wrongPaths.add("../jcr:primaryType");
        wrongPaths.add("../../jcr:primaryType");
        wrongPaths.add("../testing/jcr:primaryType");
        for (String path : wrongPaths) {
            assertNull(user.getProperty(path));
        }

        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("/testing/jcr:primaryType");
        invalidPaths.add("..");
        invalidPaths.add(".");
        invalidPaths.add(null);
        for (String invalidPath : invalidPaths) {
            try {
                assertNull(user.getProperty(invalidPath));
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testHasPropertyByInvalidRelativePath() throws NotExecutableException, RepositoryException {
        List<String> wrongPaths = new ArrayList<String>();
        wrongPaths.add("../jcr:primaryType");
        wrongPaths.add("../../jcr:primaryType");
        wrongPaths.add("../testing/jcr:primaryType");
        for (String path : wrongPaths) {
            assertFalse(user.hasProperty(path));
        }


        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("..");
        invalidPaths.add(".");
        invalidPaths.add(null);

        for (String invalidPath : invalidPaths) {
            try {
                assertFalse(user.hasProperty(invalidPath));
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testGetPropertyNames() throws NotExecutableException, RepositoryException {
        String propName = "Fullname";
        Value v = superuser.getValueFactory().createValue("Super User");
        try {
            user.setProperty(propName, v);
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        try {
            for (Iterator<String> it = user.getPropertyNames(); it.hasNext();) {
                String name = it.next();
                assertTrue(user.hasProperty(name));
                assertNotNull(user.getProperty(name));
            }
        } finally {
            // try to remove the property again even if previous calls failed.
            user.removeProperty(propName);
            superuser.save();
        }
    }

    @Test
    public void testGetPropertyNamesByRelPath() throws NotExecutableException, RepositoryException {
        String relPath = "testing/Fullname";
        Value v = superuser.getValueFactory().createValue("Super User");
        try {
            user.setProperty(relPath, v);
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        try {
            for (Iterator<String> it = user.getPropertyNames(); it.hasNext();) {
                String name = it.next();
                assertFalse("Fullname".equals(name));
            }

            for (Iterator<String> it = user.getPropertyNames("testing"); it.hasNext();) {
                String name = it.next();
                String rp = "testing/" + name;

                assertFalse(user.hasProperty(name));
                assertNull(user.getProperty(name));

                assertTrue(user.hasProperty(rp));
                assertNotNull(user.getProperty(rp));
            }
            for (Iterator<String> it = user.getPropertyNames("./testing"); it.hasNext();) {
                String name = it.next();
                String rp = "testing/" + name;

                assertFalse(user.hasProperty(name));
                assertNull(user.getProperty(name));

                assertTrue(user.hasProperty(rp));
                assertNotNull(user.getProperty(rp));
            }
        } finally {
            // try to remove the property again even if previous calls failed.
            user.removeProperty(relPath);
            superuser.save();
        }
    }

    @Test
    public void testGetPropertyNamesByInvalidRelPath() throws NotExecutableException, RepositoryException {
        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("");
        invalidPaths.add("/");
        invalidPaths.add("../");
        invalidPaths.add("../../");
        invalidPaths.add("../testing");
        invalidPaths.add("/testing");
        invalidPaths.add(null);

        for (String invalidRelPath : invalidPaths) {
            try {
                user.getPropertyNames(invalidRelPath);
                fail("Calling Authorizable#getPropertyNames with " + invalidRelPath + " must fail.");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testGetNotExistingProperty() throws RepositoryException, NotExecutableException {
        String hint = "Fullname";
        String propName = hint;
        int i = 0;
        while (user.hasProperty(propName)) {
            propName = hint + i;
            i++;
        }
        assertNull(user.getProperty(propName));
        assertFalse(user.hasProperty(propName));
    }

    @Test
    public void testSetNullPropertyRemoves() throws RepositoryException, NotExecutableException {
        Value v = superuser.getValueFactory().createValue("testValue");
        try {
            user.setProperty("testProperty", v);
            user.setProperty("testMvProperty", new Value[] {v});
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        user.setProperty("testProperty", (Value) null);
        assertFalse(user.hasProperty("testProperty"));

        user.setProperty("testMvProperty", (Value[]) null);
        assertFalse(user.hasProperty("testMvProperty"));
    }

    @Test
    public void testSingleValueToMultiValue() throws RepositoryException, NotExecutableException {
        Value v = superuser.getValueFactory().createValue("testValue");
        try {
            user.setProperty("testProperty", v);
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        user.setProperty("testProperty", new Value[] {v});
        Property p = superuser.getProperty(user.getPath() + "/testProperty");
        assertTrue(p.isMultiple());
    }

    @Test
    public void testMultiValueToSingleValue() throws RepositoryException, NotExecutableException {
        Value v = superuser.getValueFactory().createValue("testValue");
        try {
            user.setProperty("testProperty", new Value[] {v});
            superuser.save();
        } catch (RepositoryException e) {
            throw new NotExecutableException("Cannot test 'Authorizable.setProperty'.");
        }

        user.setProperty("testProperty", v);
        Property p = superuser.getProperty(user.getPath() + "/testProperty");
        assertFalse(p.isMultiple());
    }

    @Test
    public void testRemoveNotExistingProperty() throws RepositoryException, NotExecutableException {
        String hint = "Fullname";
        String propName = hint;
        int i = 0;
        while (user.hasProperty(propName)) {
            propName = hint + i;
            i++;
        }
        assertFalse(user.removeProperty(propName));
        superuser.save();
    }

    @Test
    public void testSetSpecialProperties() throws NotExecutableException, RepositoryException {
        Value v = superuser.getValueFactory().createValue("any_value");
        for (String pName : protectedUserProps.keySet()) {
            try {
                boolean isMultiValued = protectedUserProps.get(pName);
                if (isMultiValued) {
                    user.setProperty(pName, new Value[] {v});
                } else {
                    user.setProperty(pName, v);
                }
                superuser.save();
                fail("changing the '" + pName + "' property on a User should fail.");
            } catch (RepositoryException e) {
                // success
            } finally {
                superuser.refresh(false);
            }
        }

        for (String pName : protectedGroupProps.keySet()) {
            try {
                boolean isMultiValued = protectedGroupProps.get(pName);
                if (isMultiValued) {
                    group.setProperty(pName, new Value[] {v});
                } else {
                    group.setProperty(pName, v);
                }
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
        for (String pName : protectedUserProps.keySet()) {
            try {
                if (user.removeProperty(pName)) {
                    superuser.save();
                    fail("removing the '" + pName + "' property on a User should fail.");
                } // else: property not present: fine as well.
            } catch (RepositoryException e) {
                // success
            } finally {
                superuser.refresh(false);
            }
        }
        for (String pName : protectedGroupProps.keySet()) {
            try {
                if (group.removeProperty(pName)) {
                    superuser.save();
                    fail("removing the '" + pName + "' property on a Group should fail.");
                } // else: property not present. fine as well.
            } catch (RepositoryException e) {
                // success
            } finally {
                superuser.refresh(false);
            }
        }
    }

    @Test
    public void testProtectedUserProperties() throws NotExecutableException, RepositoryException {
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
            group.addMember(user);
        }

        Property p = n.getProperty(UserConstants.REP_MEMBERS);
        for (Value v : p.getValues()) {
            assertEquals(PropertyType.WEAKREFERENCE, v.getType());
        }
    }

    @Test
    public void testSetSpecialPropertiesDirectly() throws NotExecutableException, RepositoryException {
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
        Node n = getNode(user, superuser);
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
}