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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * OakAuthorizablePropertyTest: oak-level equivalent to AuthorizablePropertyTest.
 */
public class OakAuthorizablePropertyTest extends AbstractSecurityTest {

    private Root root;    
    private UserManager userMgr;
    private User user;
    private Group group;

    private Map<String, Boolean> protectedUserProps = new HashMap<String, Boolean>();
    private Map<String, Boolean> protectedGroupProps = new HashMap<String, Boolean>();
    
    @Before
    public void before() throws Exception {
        super.before();

        root = admin.getLatestRoot();
        userMgr = new UserManagerImpl(root, NamePathMapper.DEFAULT, getSecurityProvider());

        user = userMgr.createUser("testuser", "pw");
        group = userMgr.createGroup("testgroup");
        root.commit();

        protectedUserProps.put(UserConstants.REP_PASSWORD, false);
        protectedUserProps.put(UserConstants.REP_IMPERSONATORS, true);
        protectedUserProps.put(UserConstants.REP_PRINCIPAL_NAME, false);

        protectedGroupProps.put(UserConstants.REP_MEMBERS, true);
        protectedGroupProps.put(UserConstants.REP_PRINCIPAL_NAME, false);
    }

    @After
    public void after() throws Exception {
        try {
            userMgr.getAuthorizable("testuser").remove();
            userMgr.getAuthorizable("testgroup").remove();
            root.commit();
        } finally {
            super.after();
        }
    }
 
    private ValueFactory getValueFactory() {
        return new ValueFactoryImpl(root.getBlobFactory(), NamePathMapper.DEFAULT);
    }
    
    @Test
    public void testSetProperty() throws Exception {
        String propName = "Fullname";
        Value v = getValueFactory().createValue("Super User");

        user.setProperty(propName, v);
        root.commit();

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

            root.commit();
        } finally {
            // try to remove the property again even if previous calls failed.
            user.removeProperty(propName);
            root.commit();
        }
    }

    @Test
    public void testSetMultiValueProperty() throws Exception {
        String propName = "Fullname";
        Value[] v = new Value[] {getValueFactory().createValue("Super User")};
        user.setProperty(propName, v);
        root.commit();

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
            
            root.commit();
        } finally {
            // try to remove the property again even if previous calls failed.
            user.removeProperty(propName);
            root.commit();
        }
    }

    @Test
    public void testSetPropertyByRelPath() throws Exception {
        Value[] v = new Value[] {getValueFactory().createValue("Super User")};

        List<String> relPaths = new ArrayList<String>();
        relPaths.add("testing/Fullname");
        relPaths.add("testing/Email");
        relPaths.add("testing/testing/testing/Fullname");
        relPaths.add("testing/testing/testing/Email");

        for (String relPath : relPaths) {
            try {
                user.setProperty(relPath, v);
                root.commit();

                assertTrue(user.hasProperty(relPath));
                String propName = Text.getName(relPath);
                assertFalse(user.hasProperty(propName));
            } finally {
                // try to remove the property even if previous calls failed.
                user.removeProperty(relPath);
                root.commit();
            }
        }
    }

    @Test
    public void testSetPropertyInvalidRelativePath() throws Exception {
        Value[] v = new Value[] {getValueFactory().createValue("Super User")};

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
            } catch (Exception e) {
                // success.
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testGetPropertyByInvalidRelativePath() throws Exception {
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
            } catch (Exception e) {
                // success
            }
        }
    }

    @Test
    public void testHasPropertyByInvalidRelativePath() throws Exception {
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
            } catch (Exception e) {
                // success
            }
        }
    }

    @Test
    public void testGetPropertyNames() throws Exception {
        String propName = "Fullname";
        Value v = getValueFactory().createValue("Super User");
        user.setProperty(propName, v);
        root.commit();

        try {
            for (Iterator<String> it = user.getPropertyNames(); it.hasNext();) {
                String name = it.next();
                assertTrue(user.hasProperty(name));
                assertNotNull(user.getProperty(name));
            }
        } finally {
            // try to remove the property again even if previous calls failed.
            user.removeProperty(propName);
            root.commit();
        }
    }

    @Test
    public void testGetPropertyNamesByRelPath() throws Exception {
        String relPath = "testing/Fullname";
        Value v = getValueFactory().createValue("Super User");
        user.setProperty(relPath, v);
        root.commit();

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
            root.commit();
        }
    }

    @Test
    public void testGetPropertyNamesByInvalidRelPath() throws Exception {
        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("../");
        invalidPaths.add("../../");
        invalidPaths.add("../testing");
        invalidPaths.add("/testing");
        invalidPaths.add(null);

        for (String invalidRelPath : invalidPaths) {
            try {
                user.getPropertyNames(invalidRelPath);
                fail("Calling Authorizable#getPropertyNames with " + invalidRelPath + " must fail.");
            } catch (Exception e) {
                // success
            }
        }
    }

    @Test
    public void testGetNotExistingProperty() throws Exception {
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
    public void testRemoveNotExistingProperty() throws Exception {
        String hint = "Fullname";
        String propName = hint;
        int i = 0;
        while (user.hasProperty(propName)) {
            propName = hint + i;
            i++;
        }
        assertFalse(user.removeProperty(propName));
        root.commit();
    }

    @Test
    public void testSetSpecialProperties() throws Exception {
        Value v = getValueFactory().createValue("any_value");
        for (String pName : protectedUserProps.keySet()) {
            try {
                boolean isMultiValued = protectedUserProps.get(pName);
                if (isMultiValued) {
                    user.setProperty(pName, new Value[] {v});
                } else {
                    user.setProperty(pName, v);
                }
                root.commit();
                fail("changing the '" + pName + "' property on a User should fail.");
            } catch (RepositoryException e) {
                // success
            } finally {
                root.refresh();
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
                root.commit();
                fail("changing the '" + pName + "' property on a Group should fail.");
            } catch (RepositoryException e) {
                // success
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testRemoveSpecialProperties() throws Exception {
        for (String pName : protectedUserProps.keySet()) {
            try {
                if (user.removeProperty(pName)) {
                    root.commit();
                    fail("removing the '" + pName + "' property on a User should fail.");
                } // else: property not present: fine as well.
            } catch (RepositoryException e) {
                // success
            } finally {
                root.refresh();
            }
        }
        for (String pName : protectedGroupProps.keySet()) {
            try {
                if (group.removeProperty(pName)) {
                    root.commit();
                    fail("removing the '" + pName + "' property on a Group should fail.");
                } // else: property not present. fine as well.
            } catch (RepositoryException e) {
                // success
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testSingleToMultiValued() throws Exception {
        try {
            Value v = getValueFactory().createValue("anyValue");
            user.setProperty("someProp", v);
            root.commit();

            Value[] vs = new Value[] {v, v};
            user.setProperty("someProp", vs);
            root.commit();
        } finally {
            if (user.removeProperty("someProp")) {
                root.commit();
            }
        }
    }

    @Test
    public void testMultiValuedToSingle() throws Exception {
        try {
            Value v = getValueFactory().createValue("anyValue");
            Value[] vs = new Value[] {v, v};
            user.setProperty("someProp", vs);
            root.commit();

            user.setProperty("someProp", v);
            root.commit();
        } finally {
            if (user.removeProperty("someProp")) {
                root.commit();
            }
        }
    }
}