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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.state.MoveDetector;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for the hidden {@link org.apache.jackrabbit.oak.plugins.tree.TreeConstants#OAK_CHILD_ORDER} property
 */
public class HiddenPropertyTest extends AbstractOakCoreTest {

    private String[] hiddenProps = new String[] {":hiddenProp", MoveDetector.SOURCE_PATH, VersionConstants.HIDDEN_COPY_SOURCE};
    @Override
    @Before
    public void before() throws Exception {
        super.before();

        Tree a = root.getTree("/a");
        a.setProperty(":hiddenProp", "val", Type.STRING);
        a.setProperty(MoveDetector.SOURCE_PATH, "/some/path", Type.PATH);
        a.setProperty(VersionConstants.HIDDEN_COPY_SOURCE, "abc", Type.STRING);
        root.commit();
    }

    @Test
    public void testHasProperty() {
        Tree a = root.getTree("/a");
        for (String propName : hiddenProps) {
            assertFalse(a.hasProperty(propName));
        }
    }

    @Test
    public void testGetProperty() {
        Tree a = root.getTree("/a");
        for (String propName : hiddenProps) {
            assertNull(a.getProperty(propName));
        }
    }

    @Test
    public void testGetProperties() {
        Set<String> propertyNames = Sets.newHashSet(JcrConstants.JCR_PRIMARYTYPE, "aProp");

        Tree a = root.getTree("/a");
        for (PropertyState prop : a.getProperties()) {
            assertTrue(propertyNames.remove(prop.getName()));
        }
        assertTrue(propertyNames.isEmpty());
    }

    @Test
    public void testGetPropertyCount() {
        Tree a = root.getTree("/a");
        assertEquals(2, a.getPropertyCount());
    }

    @Test
    public void testGetPropertyStatus() {
        Tree a = root.getTree("/a");
        for (String propName : hiddenProps) {
            assertNull(a.getPropertyStatus(propName));
        }
    }

    @Ignore("OAK-1424") // FIXME : OAK-1424
    @Test
    public void testCreateHiddenProperty() {
        Tree a = root.getTree("/a");
        try {
            a.setProperty(":hiddenProperty", "val");
            root.commit();
            fail();
        } catch (Exception e) {
            // success
        }
    }
}