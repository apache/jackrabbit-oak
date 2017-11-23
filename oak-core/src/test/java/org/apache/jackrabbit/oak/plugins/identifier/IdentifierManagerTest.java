/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.identifier;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager.getIdentifier;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class IdentifierManagerTest {
    private static final String UUID_Y = UUIDUtils.generateUUID();
    private static final String UUID_Z1 = UUIDUtils.generateUUID();
    public static final String ID_ROOT = "/";
    public static final String ID_X1 = "/x/x1";
    public static final String ID_Y1 = UUID_Y + "/y1";
    public static final String ID_Z1 = UUID_Z1;
    public static final String PATH_X1 = "/x/x1";
    public static final String PATH_Y1 = "/y/y1";
    public static final String PATH_Z1 = "/z/z1";
    public static final String ID_INVALID = "invalid";
    public static final String ID_NON_EXISTING = UUIDUtils.generateUUID();

    private IdentifierManager identifierManager;
    private Root root;

    @Before
    public void setUp() throws CommitFailedException {
        root = new Oak()
            .with(new OpenSecurityProvider())
            .with(new InitialContent())
            .createContentSession().getLatestRoot();

        Tree tree = root.getTree("/");
        Tree x = tree.addChild("x");
        Tree y = tree.addChild("y");
        y.setProperty(JcrConstants.JCR_UUID, UUID_Y);
        Tree z = tree.addChild("z");
        x.addChild("x1");
        y.addChild("y1");
        z.addChild("z1").setProperty(JcrConstants.JCR_UUID, UUID_Z1);
        root.commit();

        identifierManager = new IdentifierManager(root);
    }

    @Test
    public void getIdentifierTest() {
        Tree rootTree = root.getTree("/");
        assertEquals(ID_ROOT, getIdentifier(rootTree));

        Tree xx1 = root.getTree(PATH_X1);
        assertEquals(ID_X1, getIdentifier(xx1));

        Tree yy1 = root.getTree(PATH_Y1);
        assertEquals(ID_Y1, getIdentifier(yy1));

        Tree zz1 = root.getTree(PATH_Z1);
        assertEquals(ID_Z1, getIdentifier(zz1));
    }

    @Test
    public void getTreeTest() {
        assertEquals("/", identifierManager.getTree(ID_ROOT).getPath());
        assertEquals(PATH_X1, identifierManager.getTree(ID_X1).getPath());
        assertEquals(PATH_Y1, identifierManager.getTree(ID_Y1).getPath());
        assertEquals(PATH_Z1, identifierManager.getTree(ID_Z1).getPath());
        assertNull(identifierManager.getTree(ID_NON_EXISTING));

        try {
            identifierManager.getTree(ID_INVALID);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) { }
    }

    @Test
    public void getPathTest() {
        assertEquals("/", identifierManager.getPath(ID_ROOT));
        assertEquals(PATH_X1, identifierManager.getPath(ID_X1));
        assertEquals(PATH_Y1, identifierManager.getPath(ID_Y1));
        assertEquals(PATH_Z1, identifierManager.getPath(ID_Z1));
        assertNull(identifierManager.getPath(ID_NON_EXISTING));

        try {
            identifierManager.getPath(ID_INVALID);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) { }
    }

    @Test
    public void getPathFromPropertyTest() {
        assertEquals("/y", identifierManager.getPath(createReferenceProperty(UUID_Y)));
        assertEquals(PATH_Z1, identifierManager.getPath(createReferenceProperty(UUID_Z1)));
        assertNull(identifierManager.getPath(createReferenceProperty(ID_NON_EXISTING)));
        assertNull(identifierManager.getPath(createReferenceProperty(ID_INVALID)));

        try {
            identifierManager.getPath(PropertyStates.createProperty("any", "any"));
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) { }
    }

    private static PropertyState createReferenceProperty(String value) {
        return PropertyStates.createProperty("ref", value, Type.REFERENCE);
    }

}
