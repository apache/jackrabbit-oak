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

package org.apache.jackrabbit.oak.plugins.index;

import java.util.Collections;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.oak.plugins.index.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.plugins.index.PathFilter.PROP_INCLUDED_PATHS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PathFilterTest {

    @Test
    public void exclude() throws Exception{
        PathFilter p = new PathFilter(of("/"), of("/etc"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/a"));
        assertEquals(PathFilter.Result.EXCLUDE, p.doFiler("/etc"));
        assertEquals(PathFilter.Result.EXCLUDE, p.doFiler("/etc/workflow"));
    }

    @Test
    public void include() throws Exception{
        PathFilter p = new PathFilter(of("/content", "/etc"), of("/etc/workflow/instance"));
        assertEquals(PathFilter.Result.TRAVERSE, p.doFiler("/"));
        assertEquals(PathFilter.Result.EXCLUDE, p.doFiler("/var"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/content"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/content/example"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/etc"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/etc/workflow"));
        assertEquals(PathFilter.Result.EXCLUDE, p.doFiler("/etc/workflow/instance"));
        assertEquals(PathFilter.Result.EXCLUDE, p.doFiler("/etc/workflow/instance/1"));
    }

    @Test
    public void emptyConfig() throws Exception{
        NodeBuilder root = EMPTY_NODE.builder();
        PathFilter p = PathFilter.from(root);
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/a"));
    }

    @Test
    public void config() throws Exception{
        NodeBuilder root = EMPTY_NODE.builder();
        root.setProperty(createProperty(PROP_INCLUDED_PATHS, of("/etc"), Type.STRINGS));
        root.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/etc/workflow"), Type.STRINGS));
        PathFilter p = PathFilter.from(root);
        assertEquals(PathFilter.Result.TRAVERSE, p.doFiler("/"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/etc"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/etc/a"));
        assertEquals(PathFilter.Result.EXCLUDE, p.doFiler("/etc/workflow"));
        assertEquals(PathFilter.Result.EXCLUDE, p.doFiler("/etc/workflow/1"));
    }

    @Test
    public void configOnlyExclude() throws Exception{
        NodeBuilder root = EMPTY_NODE.builder();
        root.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/etc/workflow"), Type.STRINGS));
        PathFilter p = PathFilter.from(root);
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/etc"));
        assertEquals(PathFilter.Result.INCLUDE, p.doFiler("/etc/a"));
        assertEquals(PathFilter.Result.EXCLUDE, p.doFiler("/etc/workflow"));
        assertEquals(PathFilter.Result.EXCLUDE, p.doFiler("/etc/workflow/1"));
    }

    @Test
    public void invalid() throws Exception{
        try {
            new PathFilter(Collections.<String>emptyList(), of("/etc"));
            fail();
        } catch (IllegalStateException ignore){

        }

        try {
            new PathFilter(of("/etc/workflow"), of("/etc"));
            fail();
        } catch (IllegalStateException ignore){

        }

        try {
            new PathFilter(Collections.<String>emptyList(), Collections.<String>emptyList());
            fail();
        } catch (IllegalStateException ignore){

        }
    }
}
