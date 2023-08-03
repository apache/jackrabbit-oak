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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedWriter;
import java.io.StringWriter;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SimpleFlatFileUtilTest {

    private final String LINE_SEPARATOR = System.getProperty("line.separator");

    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore dns;

    private StringWriter sw;

    private BufferedWriter bw;

    @Before
    public void setup() throws Exception {
        DocumentMK.Builder builder = builderProvider.newBuilder();
        builder.setDocumentStore(new MemoryDocumentStore());
        dns = builder.getNodeStore();
        sw = new StringWriter();
        bw = new BufferedWriter(sw);
    }

    @Test
    public void testEmpty() throws Exception {
        SimpleFlatFileUtil.createFlatFileFor(dns.getRoot(), bw);
        bw.close();
        assertEquals("/|{}" + LINE_SEPARATOR, sw.toString());
    }

    @Test
    public void testSimple() throws Exception {
        NodeBuilder b = dns.getRoot().builder();
        b.child("child");
        b.setProperty("prop", "value");
        dns.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        SimpleFlatFileUtil.createFlatFileFor(dns.getRoot(), bw);
        bw.close();
        assertEquals(
                "/|{\"prop\":\"value\"}" + LINE_SEPARATOR + "/child|{}" + LINE_SEPARATOR,
                sw.toString());
    }

    @Test
    public void testPropertiesSorting() throws Exception {
        NodeBuilder b = dns.getRoot().builder();
        b.setProperty("g", "a");
        b.setProperty("a", "z");
        b.setProperty("c", "t");
        dns.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        SimpleFlatFileUtil.createFlatFileFor(dns.getRoot(), bw);
        bw.close();
        assertEquals("/|{\"a\":\"z\",\"c\":\"t\",\"g\":\"a\"}" + LINE_SEPARATOR,
                sw.toString());
    }
}
