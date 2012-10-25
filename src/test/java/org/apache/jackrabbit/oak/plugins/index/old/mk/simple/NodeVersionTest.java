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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.SimpleKernelImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test moving nodes.
 */
public class NodeVersionTest {

    private final MicroKernel mk = new SimpleKernelImpl("mem:NodeVersionTest");

    @Test
    public void nodeVersion() {
        String head = mk.getHeadRevision();
        head = mk.commit("/:root/head/config", "^ \"nodeVersion\": true", head, "");

        head = mk.commit("/", "+ \"test1\": { \"id\": 1 }", head, "");
        head = mk.commit("/", "+ \"test2\": { \"id\": 1 }", head, "");

        NodeImpl n = NodeImpl.parse(mk.getNodes("/", head, 1, 0, -1, null));
        String vra = n.getNodeVersion();
        String v1a = n.getNode("test1").getNodeVersion();
        String v2a = n.getNode("test2").getNodeVersion();

        // changes the node version
        head = mk.commit("/", "^ \"test2/id\": 2", head, "");

        n = NodeImpl.parse(mk.getNodes("/", head, 1, 0, -1, null));
        String vrb = n.getNodeVersion();
        String v1b = n.getNode("test1").getNodeVersion();
        String v2b = n.getNode("test2").getNodeVersion();

        assertFalse(vra.equals(vrb));
        assertEquals(v1a, v1b);
        assertFalse(v2a.equals(v2b));
    }

}
