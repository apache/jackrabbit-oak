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
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.SimpleKernelImpl;
import org.junit.Test;

/**
 * {@link MicroKernel} test cases that currently only work against the
 * {@link SimpleKernelImpl} implementation.
 * <p>
 * TODO: Review these to see if they rely on implementation-specific
 * functionality or if they should be turned into generic integration
 * tests and the respective test failures in other MK implementations
 * fixed.
 */
public class SimpleKernelTest {

    private final MicroKernel mk = new SimpleKernelImpl("mem:SimpleKernelTest");

    @Test
    public void reorderNode() {
        String head = mk.getHeadRevision();
        String node = "reorderNode_" + System.currentTimeMillis();
        head = mk.commit("/", "+\"" + node + "\" : {\"a\":{}, \"b\":{}, \"c\":{}}", head, "");
        // System.out.println(mk.getNodes('/' + node, head).replaceAll("\"", "").replaceAll(":childNodeCount:.", ""));

        head = mk.commit("/", ">\"" + node + "/a\" : {\"before\":\"" + node + "/c\"}", head, "");
        // System.out.println(mk.getNodes('/' + node, head).replaceAll("\"", "").replaceAll(":childNodeCount:.", ""));
    }

    @Test
    public void doubleDelete() {
        String head = mk.getHeadRevision();
        head = mk.commit("/", "+\"a\": {}", head, "");
        mk.commit("/", "-\"a\"", head, "");
        mk.commit("/", "-\"a\"", head, "");
    }

}
