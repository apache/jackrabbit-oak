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

package org.apache.jackrabbit.oak.plugins.index.inventory;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.google.common.collect.Lists;
import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexDefinitionPrinterTest {
    private NodeStore store = new MemoryNodeStore();
    private IndexPathService pathService = mock(IndexPathService.class);
    private IndexDefinitionPrinter printer = new IndexDefinitionPrinter(store, pathService);

    @Test
    public void printer() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").setProperty(":foo", "bar");
        builder.child("a").setProperty(":childOrder", "bar");
        builder.child("b").child("c").setProperty("foo", "bar");
        builder.child("b").child("c").setProperty(":foo", "bar");
        builder.child("b").child(":d").setProperty("foo", "bar");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        when(pathService.getIndexPaths()).thenReturn(Lists.newArrayList("/a", "/b"));

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        printer.print(pw, Format.JSON, false);

        pw.flush();

        String json = sw.toString();
        //If there is any error in rendered json
        //exception would fail the test
        JSONObject o = (JSONObject) JSONValue.parseWithException(json);
        assertNull(o.get(":d"));

        JSONObject a = (JSONObject) o.get("/a");
        assertNotNull(a.get("foo"));
        assertNotNull(a.get(":childOrder"));
    }
}