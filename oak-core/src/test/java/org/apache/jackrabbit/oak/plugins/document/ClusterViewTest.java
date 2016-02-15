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
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.junit.Test;

/** Simple paranoia tests for constructor and getters of ClusterViewImpl **/
public class ClusterViewTest {

    @Test
    public void testConstructor() throws Exception {
        final Integer viewId = 3;
        final String clusterViewId = UUID.randomUUID().toString();
        final Integer instanceId = 2;
        final Set<Integer> emptyInstanceIds = new HashSet<Integer>();
        final Set<Integer> instanceIds = new HashSet<Integer>();
        instanceIds.add(1);
        final Set<Integer> deactivating = new HashSet<Integer>();
        final Set<Integer> inactive = new HashSet<Integer>();
        try {
            new ClusterView(-1, true, clusterViewId, instanceId, instanceIds, deactivating, inactive);
            fail("should complain");
        } catch (IllegalStateException e) {
            // ok
        }
        try {
            new ClusterView(viewId, true, null, instanceId, instanceIds, deactivating, inactive);
            fail("should complain");
        } catch (IllegalStateException e) {
            // ok
        }
        try {
            new ClusterView(viewId, true, clusterViewId, -1, instanceIds, deactivating, inactive);
            fail("should complain");
        } catch (IllegalStateException e) {
            // ok
        }
        try {
            new ClusterView(viewId, true, clusterViewId, instanceId, emptyInstanceIds, deactivating, inactive);
            fail("should complain");
        } catch (IllegalStateException e) {
            // ok
        }
        try {
            new ClusterView(viewId, true, clusterViewId, instanceId, null, deactivating, inactive);
            fail("should complain");
        } catch (IllegalStateException e) {
            // ok
        }
        try {
            new ClusterView(viewId, true, clusterViewId, instanceId, instanceIds, null, inactive);
            fail("should complain");
        } catch (Exception e) {
            // ok
        }
        try {
            new ClusterView(viewId, true, clusterViewId, instanceId, instanceIds, deactivating, null);
            fail("should complain");
        } catch (Exception e) {
            // ok
        }
        final Set<Integer> nonEmptyDeactivating = new HashSet<Integer>();
        nonEmptyDeactivating.add(3);
        new ClusterView(viewId, false, clusterViewId, instanceId, instanceIds, nonEmptyDeactivating, inactive);
        new ClusterView(viewId, true, clusterViewId, instanceId, instanceIds, nonEmptyDeactivating, inactive);
        // should not complain about:
        new ClusterView(viewId, true, clusterViewId, instanceId, instanceIds, deactivating, inactive);
    }

    @Test
    public void testGetters() throws Exception {
        final Integer viewId = 3;
        final String clusterViewId = UUID.randomUUID().toString();
        final Set<Integer> instanceIds = new HashSet<Integer>();
        instanceIds.add(1);
        final Integer instanceId = 2;
        final Set<Integer> deactivating = new HashSet<Integer>();
        final Set<Integer> inactive = new HashSet<Integer>();
        final ClusterView cv = new ClusterView(viewId, true, clusterViewId, instanceId, instanceIds, deactivating, inactive);
        assertNotNull(cv);
        assertTrue(cv.asDescriptorValue().length() > 0);
        assertTrue(cv.toString().length() > 0);
    }

    @Test
    public void testOneActiveOnly() throws Exception {
        String clusterId = UUID.randomUUID().toString();
        ClusterViewBuilder builder = new ClusterViewBuilder(10, 21);
        ClusterView view = builder.active(21).asView(clusterId);

        // {"seq":10,"id":"35f60ed3-508d-4a81-b812-89f07f57db20","me":2,"active":[2],"deactivating":[],"inactive":[3]}
        JsonObject o = asJsonObject(view);
        Map<String, String> props = o.getProperties();
        assertEquals("10", props.get("seq"));
        assertEquals(clusterId, unwrapString(props.get("id")));
        assertEquals("21", props.get("me"));
        assertEquals(asJsonArray(21), props.get("active"));
        assertEquals(asJsonArray(), props.get("deactivating"));
        assertEquals(asJsonArray(), props.get("inactive"));
    }

    @Test
    public void testOneActiveOneInactive() throws Exception {
        String clusterId = UUID.randomUUID().toString();
        ClusterViewBuilder builder = new ClusterViewBuilder(10, 2);
        ClusterView view = builder.active(2).inactive(3).asView(clusterId);

        // {"seq":10,"id":"35f60ed3-508d-4a81-b812-89f07f57db20","me":2,"active":[2],"deactivating":[],"inactive":[3]}
        JsonObject o = asJsonObject(view);
        Map<String, String> props = o.getProperties();
        assertEquals("10", props.get("seq"));
        assertEquals(clusterId, unwrapString(props.get("id")));
        assertEquals("2", props.get("me"));
        assertEquals(asJsonArray(2), props.get("active"));
        assertEquals(asJsonArray(), props.get("deactivating"));
        assertEquals(asJsonArray(3), props.get("inactive"));
    }

    @Test
    public void testSeveralActiveOneInactive() throws Exception {
        String clusterId = UUID.randomUUID().toString();
        ClusterViewBuilder builder = new ClusterViewBuilder(10, 2);
        ClusterView view = builder.active(2, 5, 6).inactive(3).asView(clusterId);

        // {"seq":10,"id":"35f60ed3-508d-4a81-b812-89f07f57db20","me":2,"active":[2],"deactivating":[],"inactive":[3]}
        JsonObject o = asJsonObject(view);
        Map<String, String> props = o.getProperties();
        assertEquals("10", props.get("seq"));
        assertEquals("true", props.get("final"));
        assertEquals(clusterId, unwrapString(props.get("id")));
        assertEquals("2", props.get("me"));
        assertEquals(asJsonArray(2, 5, 6), props.get("active"));
        assertEquals(asJsonArray(), props.get("deactivating"));
        assertEquals(asJsonArray(3), props.get("inactive"));
    }

    @Test
    public void testOneActiveSeveralInactive() throws Exception {
        String clusterId = UUID.randomUUID().toString();
        ClusterViewBuilder builder = new ClusterViewBuilder(10, 2);
        ClusterView view = builder.active(2).inactive(3, 4, 5, 6).asView(clusterId);

        // {"seq":10,"id":"35f60ed3-508d-4a81-b812-89f07f57db20","me":2,"active":[2],"deactivating":[],"inactive":[3]}
        JsonObject o = asJsonObject(view);
        Map<String, String> props = o.getProperties();
        assertEquals("10", props.get("seq"));
        assertEquals("true", props.get("final"));
        assertEquals(clusterId, unwrapString(props.get("id")));
        assertEquals("2", props.get("me"));
        assertEquals(asJsonArray(2), props.get("active"));
        assertEquals(asJsonArray(), props.get("deactivating"));
        assertEquals(asJsonArray(3, 4, 5, 6), props.get("inactive"));
    }

    @Test
    public void testWithRecoveringOnly() throws Exception {
        String clusterId = UUID.randomUUID().toString();
        ClusterViewBuilder builder = new ClusterViewBuilder(10, 2);
        ClusterView view = builder.active(2, 3).recovering(4).inactive(5, 6).asView(clusterId);

        JsonObject o = asJsonObject(view);
        Map<String, String> props = o.getProperties();
        assertEquals("10", props.get("seq"));
        assertEquals("true", props.get("final"));
        assertEquals(clusterId, unwrapString(props.get("id")));
        assertEquals("2", props.get("me"));
        assertEquals(asJsonArray(2, 3), props.get("active"));
        assertEquals(asJsonArray(4), props.get("deactivating"));
        assertEquals(asJsonArray(5, 6), props.get("inactive"));
    }

    @Test
    public void testWithRecoveringAndBacklog() throws Exception {
        String clusterId = UUID.randomUUID().toString();
        ClusterViewBuilder builder = new ClusterViewBuilder(10, 2);
        ClusterView view = builder.active(2, 3).recovering(4).inactive(5, 6).backlogs(5).asView(clusterId);

        JsonObject o = asJsonObject(view);
        Map<String, String> props = o.getProperties();
        assertEquals("10", props.get("seq"));
        assertEquals(clusterId, unwrapString(props.get("id")));
        assertEquals("2", props.get("me"));
        assertEquals("false", props.get("final"));
        assertEquals(asJsonArray(2, 3), props.get("active"));
        assertEquals(asJsonArray(4, 5), props.get("deactivating"));
        assertEquals(asJsonArray(6), props.get("inactive"));
    }

    @Test
    public void testBacklogButNotInactive() throws Exception {
        String clusterId = UUID.randomUUID().toString();
        ClusterViewBuilder builder = new ClusterViewBuilder(10, 2);
        try {
            ClusterView view = builder.active(2, 3).backlogs(5).asView(clusterId);
            fail("should complain");
        } catch (Exception ok) {
            // ok
        }
    }

    private JsonObject asJsonObject(final ClusterView view) {
        final String json = view.asDescriptorValue();
//        System.out.println(json);
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('{');
        JsonObject o = JsonObject.create(t);
        return o;
    }

    private String unwrapString(String stringWithQuotes) {
        // TODO: I'm not really sure why the JsonObject parses this string
        // including the "
        // perhaps that's rather a bug ..
        assertTrue(stringWithQuotes.startsWith("\""));
        assertTrue(stringWithQuotes.endsWith("\""));
        return stringWithQuotes.substring(1, stringWithQuotes.length() - 1);
    }

    static String asJsonArray(int... ids) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < ids.length; i++) {
            int anId = ids[i];
            if (i != 0) {
                sb.append(",");
            }
            sb.append(String.valueOf(anId));
        }
        sb.append("]");
        return sb.toString();
    }
}
