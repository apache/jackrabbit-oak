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

import java.util.ArrayList;
import java.util.List;
import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.Property;

import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

/**
 * @since OAK 1.0 : jcr:read is an aggregation of read property and node privileges.
 */
public class ReadPropertyTest extends AbstractEvaluationTest {

    @Test
    public void testReadProperty() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.JCR_READ));
        allow(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        assertFalse(testSession.nodeExists(path));
        assertFalse(testSession.itemExists(path));
        assertFalse(testSession.nodeExists(childNPath));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            assertTrue(testSession.itemExists(pPath));
            assertTrue(testSession.propertyExists(pPath));
            Property p = testSession.getProperty(pPath);
            assertEquals(pPath, p.getPath());
        }
    }

    @Test
    public void testReadProperty2() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_NODES));

        assertFalse(testSession.nodeExists(path));
        assertFalse(testSession.itemExists(path));
        assertFalse(testSession.nodeExists(childNPath));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            assertTrue(testSession.itemExists(pPath));
            assertTrue(testSession.propertyExists(pPath));
            Property p = testSession.getProperty(pPath);
        }
    }

    @Test
    public void testGetParent() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_NODES));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            Property p = testSession.getProperty(pPath);
            try {
                Node n = p.getParent();
                fail();
            } catch (AccessDeniedException e) {
                // success
            }
        }
    }

    @Test
    public void testGetPath() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.JCR_READ));
        allow(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            Property p = testSession.getProperty(pPath);
            assertEquals(pPath, p.getPath());
        }
    }

    @Test
    public void testGetStatus() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.JCR_READ));
        allow(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            Property p = testSession.getProperty(pPath);
            assertFalse(p.isModified());
            assertFalse(p.isNew());
        }
    }
}