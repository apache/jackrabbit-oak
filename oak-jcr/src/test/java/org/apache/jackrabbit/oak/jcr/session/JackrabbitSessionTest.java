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
package org.apache.jackrabbit.oak.jcr.session;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.GuestCredentials;
import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;

import static org.mockito.Mockito.mock;

public class JackrabbitSessionTest extends AbstractJCRTest {
    
    private JackrabbitSession s;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        if (superuser instanceof JackrabbitSession) {
            s = (JackrabbitSession) superuser;
        } else {
            throw new NotExecutableException("JackrabbitSession expected");
        }
    }

    public void testGetParentOrNullRootNode() throws Exception {
        assertNull(s.getParentOrNull(s.getRootNode()));
    }

    public void testGetParentOrNull() throws Exception {
        Node n = s.getNode(testRoot);
        assertEquivalentNode(n, s.getParentOrNull(n.getProperty(Property.JCR_PRIMARY_TYPE)));
        assertEquivalentNode(n.getParent(), s.getParentOrNull(n));
    }
    
    private static void assertEquivalentNode(@NotNull Node expected, @Nullable Node result) throws Exception {
        assertNotNull(result);
        assertEquals(expected.getPath(), result.getPath());
    }
    
    public void testGetParentOrNullSessionMismatch() throws Exception {
        JackrabbitSession guest = (JackrabbitSession) getHelper().getRepository().login(new GuestCredentials());
        try {
            guest.getParentOrNull(s.getNode(testRoot));
            fail("RepositoryException expected");
        } catch (RepositoryException e) {
            // success
        } finally {
            guest.logout();
        }
    }

    public void testGetParentOrNullImplMismatch() {
        try {
            Item item = mock(Item.class);
            s.getParentOrNull(item);
            fail("RepositoryException expected");
        } catch (RepositoryException e) {
            // success
        }
    }
    
    
}