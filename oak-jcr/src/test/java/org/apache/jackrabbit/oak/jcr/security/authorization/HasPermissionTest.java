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

import java.util.List;
import java.util.Map;

import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;

/**
 * Testing {@link Session#hasPermission(String,String)} and {@link JackrabbitSession#hasPermission(String, String...)}
 */
public class HasPermissionTest extends AbstractEvaluationTest {

    public void testEmpty() throws Exception {
        List<String> paths = ImmutableList.of(
                "/", path, childPPath, path + "/rep:policy",
                "/nonExisting", path + "/nonExisting");

        for (String p : paths) {
            assertTrue(testSession.hasPermission(p, ""));
            assertTrue(testSession.hasPermission(p, ",,"));
            assertTrue(((JackrabbitSession) testSession).hasPermission(p, new String[0]));
            assertTrue(((JackrabbitSession) testSession).hasPermission(p, new String[]{""}));
            assertTrue(((JackrabbitSession) testSession).hasPermission(p, new String[]{"", ""}));
            assertTrue(((JackrabbitSession) testSession).hasPermission(p, "", ""));
        }
    }

    public void testSingle() throws Exception {
        Map<String, Boolean> map = Maps.newHashMap();
        map.put("/", true);
        map.put(path, true);
        map.put(childPPath, true);
        map.put(path + "/rep:policy", false);
        map.put("/nonExisting", true);
        map.put(path + "/nonExisting", true);

        for (String p : map.keySet()) {
            boolean expected = map.get(p);
            assertEquals(p, expected, testSession.hasPermission(p, Session.ACTION_READ));
            assertEquals(p, expected, ((JackrabbitSession) testSession).hasPermission(p, new String[]{Session.ACTION_READ}));
        }
    }

    public void testDuplicate() throws Exception {
        Map<String, Boolean> map = Maps.newHashMap();
        map.put("/", true);
        map.put(path, true);
        map.put(childPPath, true);
        map.put(path + "/rep:policy", false);
        map.put("/nonExisting", true);
        map.put(path + "/nonExisting", true);

        for (String p : map.keySet()) {
            boolean expected = map.get(p);
            assertEquals(p, expected, testSession.hasPermission(p, Session.ACTION_READ + "," + Permissions.getString(Permissions.READ)));
            assertEquals(p, expected, ((JackrabbitSession) testSession).hasPermission(p, new String[]{Session.ACTION_READ, Session.ACTION_READ}));
            assertEquals(p, expected, ((JackrabbitSession) testSession).hasPermission(p, Session.ACTION_READ, Session.ACTION_READ));
            assertEquals(p, expected, ((JackrabbitSession) testSession).hasPermission(p, new String[]{Session.ACTION_READ, Permissions.PERMISSION_NAMES.get(Permissions.READ)}));
            assertEquals(p, expected, ((JackrabbitSession) testSession).hasPermission(p, Session.ACTION_READ, Permissions.PERMISSION_NAMES.get(Permissions.READ)));
        }
    }

    public void testMultiple() throws Exception {
        List<String> paths = ImmutableList.of(
                "/", path, childPPath, path + "/rep:policy",
                "/nonExisting", path + "/nonExisting");

        for (String p : paths) {
            assertFalse(testSession.hasPermission(p, Session.ACTION_READ + "," + Session.ACTION_SET_PROPERTY));
            assertFalse(testSession.hasPermission(p, Session.ACTION_READ + "," + Permissions.getString(Permissions.ADD_PROPERTY)));

            assertFalse(((JackrabbitSession) testSession).hasPermission(p, Session.ACTION_READ, Session.ACTION_SET_PROPERTY));
            assertFalse(((JackrabbitSession) testSession).hasPermission(p, Session.ACTION_READ, JackrabbitSession.ACTION_ADD_PROPERTY));

            assertFalse(testSession.hasPermission(p, Session.ACTION_READ + "," + JackrabbitSession.ACTION_READ_ACCESS_CONTROL));
            assertFalse(((JackrabbitSession) testSession).hasPermission(p, Session.ACTION_READ, JackrabbitSession.ACTION_READ_ACCESS_CONTROL));
        }
    }
}