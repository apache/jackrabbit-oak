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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.security.Privilege;

import org.junit.Test;

/**
 * Specific tests for the individual property modification permissions:
 * - add
 * - modify
 * - remove
 *
 * @since OAK 1.0
 */
public class WritePropertyTest extends AbstractEvaluationTest {

    @Test
    public void testAddProperty() throws Exception {
        // grant 'testUser' ADD_PROPERTIES privileges at 'path'
        Privilege[] privileges = privilegesFromNames(new String[] {"rep:addProperties"});
        allow(path, privileges);

        /*
         testuser must now have
         - ADD_PROPERTIES permission
         - no other write permission
        */
        assertHasPrivilege(path, Privilege.JCR_MODIFY_PROPERTIES, false);
        assertHasPrivilege(path, "rep:addProperties", true);
        assertHasPrivilege(path, "rep:removeProperties", false);
        assertHasPrivilege(path, "rep:alterProperties", false);

        // set_property action for non-existing property is translated to "add_properties" permission
        String propertyPath = path + "/newProperty";
        assertTrue(testSession.hasPermission(propertyPath, Session.ACTION_SET_PROPERTY));

        // creating the property must succeed
        Node testN = testSession.getNode(path);
        testN.setProperty("newProperty", "value");
        testSession.save();

        // now property exists -> 'set_property' actions is no longer granted
        assertFalse(testSession.hasPermission(propertyPath, Session.ACTION_SET_PROPERTY));
        assertFalse(testSession.hasPermission(propertyPath, Session.ACTION_REMOVE));

        // modifying or removing the new property must fail
        try {
            testN.setProperty("newProperty", "modified");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }

        try {
            testN.getProperty("newProperty").setValue("modified");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }

        try {
            testN.setProperty("newProperty", (String) null);
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
            testSession.refresh(false);
        }

        try {
            testN.getProperty("newProperty").remove();
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
            testSession.refresh(false);
        }
    }

    @Test
    public void testModifyProperty() throws Exception {
        // grant 'testUser' ALTER_PROPERTIES privileges at 'path'
        Privilege[] privileges = privilegesFromNames(new String[] {"rep:alterProperties"});
        allow(path, privileges);

        /*
         testuser must now have
         - MODIFY_PROPERTY_PROPERTIES permission
         - no other write permission
        */
        assertHasPrivilege(path, Privilege.JCR_MODIFY_PROPERTIES, false);
        assertHasPrivilege(path, "rep:addProperties", false);
        assertHasPrivilege(path, "rep:removeProperties", false);
        assertHasPrivilege(path, "rep:alterProperties", true);

        // set_property action for non-existing property is translated to
        // "add_properties" permission
        String propertyPath = path + "/newProperty";
        assertFalse(testSession.hasPermission(propertyPath, Session.ACTION_SET_PROPERTY));

        // creating a new property must fail
        Node testN = testSession.getNode(path);
        try {
            testN.setProperty("newProperty", "value");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
            testSession.refresh(false);
        }

        superuser.getNode(path).setProperty("newProperty", "value");
        superuser.save();
        testSession.refresh(false);

        // property exists -> 'set_property' actions is granted, 'remove' is denied
        assertTrue(testSession.hasPermission(propertyPath, Session.ACTION_SET_PROPERTY));
        assertFalse(testSession.hasPermission(propertyPath, Session.ACTION_REMOVE));

        // modifying the new property must succeed
        testN.setProperty("newProperty", "modified");
        testSession.save();

        testN.getProperty("newProperty").setValue("modified2");
        testSession.save();

        // removing the property must fail
        try {
            testN.setProperty("newProperty", (String) null);
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
            testSession.refresh(false);
        }

        try {
            testN.getProperty("newProperty").remove();
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
            testSession.refresh(false);
        }
    }

    @Test
    public void testRemoveProperty() throws Exception {
        // grant 'testUser' REMOVE_PROPERTIES privileges at 'path'
        Privilege[] privileges = privilegesFromNames(new String[] {"rep:removeProperties"});
        allow(path, privileges);

        /*
         testuser must now have
         - REMOVE_PROPERTY_PROPERTIES permission
         - no other write permission
        */
        assertHasPrivilege(path, Privilege.JCR_MODIFY_PROPERTIES, false);
        assertHasPrivilege(path, "rep:addProperties", false);
        assertHasPrivilege(path, "rep:removeProperties", true);
        assertHasPrivilege(path, "rep:alterProperties", false);

        // set_property action for non-existing property is translated to
        // "add_properties" permission -> denied
        String propertyPath = path + "/newProperty";
        assertFalse(testSession.hasPermission(propertyPath, Session.ACTION_SET_PROPERTY));

        // creating a new property must fail
        Node testN = testSession.getNode(path);
        try {
            testN.setProperty("newProperty", "value");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
            testSession.refresh(false);
        }

        superuser.getNode(path).setProperty("newProperty", "value");
        superuser.save();
        testSession.refresh(false);

        // now property exists -> 'set_property' actions is denied, 'remove' is granted
        assertFalse(testSession.hasPermission(propertyPath, Session.ACTION_SET_PROPERTY));
        assertTrue(testSession.hasPermission(propertyPath, Session.ACTION_REMOVE));

        // modifying the new property must fail
        try {
            testN.setProperty("newProperty", "modified");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }

        try {
            testN.getProperty("newProperty").setValue("modified");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }

        // removing the property must succeed
        testN.getProperty("newProperty").remove();
        testSession.save();
    }

    @Test
    public void testRemoveProperty2() throws Exception {
        // grant 'testUser' REMOVE_PROPERTIES privileges at 'path'
        Privilege[] privileges = privilegesFromNames(new String[] {"rep:removeProperties"});
        allow(path, privileges);

        superuser.getNode(path).setProperty("newProperty", "value");
        superuser.save();
        testSession.refresh(false);

        // removing by setting to null must succeeed
        testSession.getNode(path).setProperty("newProperty", (String) null);
        testSession.save();
    }
}