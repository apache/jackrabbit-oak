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
package org.apache.jackrabbit.oak.run;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.jackrabbit.oak.console.NodeStoreFixture;
import org.apache.jackrabbit.oak.console.NodeStoreOpener;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class IndexTest {

    @Test
    public void simple() throws Exception {
        assertCommand(
                "hello", 
                "{'print':'hello'}");
        assertCommand(
                "false", 
                "{'print':false}");
        assertCommand("1\n2\n3", 
                "{'$x':[1, 2, 3]}",
                "{'for':'$x', 'do': [{'print': '$x'}]}");
        assertCommand("x1\nx2\nx3", 
                "{'$myFunction':[{'$y': 'x', '+': '$x'}, {'print':'$y'}]}",                
                "{'$x':[1, 2, 3]}",
                "{'for':'$x', 'do': '$myFunction'}");
        assertCommand("2\n4\n8", 
                "{'$x':1}",
                "{'loop':[{'$x': '$x', '+':'$x'}, {'print': '$x'}, {'$break': true, 'if': '$x', '=': 8}]}");
        assertCommand("b\nd", 
                "{'$x':1}",
                "{'print':'a', 'if':'$x', '=':null}",
                "{'print':'b', 'if':'$x', '=':1}",
                "{'print':'c', 'if':null, '=':1}",
                "{'print':'d', 'if':null, '=':null}");
        assertCommand("10\n10", 
                "{'$x':1}",
                "{'$$x':10}",
                "{'print':'$1'}",
                "{'print':'$$x'}");
        assertCommand("1\nnull\n1\n2\na1", 
                "{'$x':1, '+':null}",
                "{'print':'$x'}",
                "{'$x':null, '+':null}",
                "{'print':'$x'}",
                "{'$x':null, '+':1}",
                "{'print':'$x'}",
                "{'$x':1, '+':1}",
                "{'print':'$x'}",
                "{'$x':'a', '+':'1'}",
                "{'print':'$x'}");
    }
    
    private static NodeStoreFixture memoryFixture() {
        return new NodeStoreFixture() {

            @Override
            public void close() throws IOException {
                // ignore
            }

            @Override
            public NodeStore getStore() {
                try {
                    return SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            
        };
    }
    
    @Test
    public void readWrite() throws Exception {
        IndexCommand index = new IndexCommand();
        try (NodeStoreFixture fixture = memoryFixture();
                ) {
            NodeStore store = fixture.getStore();
            index.session = NodeStoreOpener.openSession(store);
            assertCommand(index, "",
                    "{'addNode':'/foo', 'node':{'jcr:primaryType': 'nt:unstructured', 'x': 1, 'y':{}}}", 
                    "{'session': 'save'}");
            assertCommand(index, 
                    "/foo\n" + 
                    "/jcr:system\n" +
                    "/oak:index\n" +
                    "/rep:security",
                    "{'xpath':'/jcr:root/* order by @jcr:path'}");
            assertCommand(index, 
                    "/oak:index/counter",
                    "{'xpath':'/jcr:root//element(*, oak:QueryIndexDefinition)[@type=`counter`] " + 
                        "order by @jcr:path'}");
            assertCommand(index, 
                    "[nt:unstructured] as [a] /* property test = 1 " + 
                            "where ([a].[x] = 1) and (isdescendantnode([a], [/])) */",
                    "{'addNode':'/oak:index/test', 'node':{ " + 
                        "'jcr:primaryType':'oak:QueryIndexDefinition', " + 
                        "'type':'property', " + 
                        "'reindex':true, " +
                        "'entryCount': 1, " +
                        "'{Name}declaringNodeTypes': ['nt:unstructured'], " +
                        "'{Name}propertyNames':['x'] " +
                        "}}",
                    "{'session':'save'}",
                    "{'xpath':'explain /jcr:root//element(*, nt:unstructured)[@x=1]'}",
                    "{'xpath':'/jcr:root//element(*, nt:unstructured)[@x=2]'}"
                    );
            assertCommand(index, 
                    "50",
                    "{'addNode':'/foo/test', 'node':{'jcr:primaryType': 'oak:Unstructured', 'child':{}}}",
                    "{'$x':1}",
                    "{'loop':[" + 
                            "{'$p': '/foo/test/child/n', '+': '$x'}, " + 
                            "{'addNode': '$p', 'node': {'x': '$x', 'jcr:primaryType': 'nt:unstructured'}}, " + 
                            "{'session':'save'}, " +
                            "{'$x': '$x', '+':1}, " + 
                            "{'$break': true, 'if': '$x', '=': 100}]}",
                    "{'session':'save'}",
                    "{'xpath':'/jcr:root//element(*, nt:unstructured)[@x<50]', 'quiet':true}",
                    "{'$y':0}",
                    "{'for':'$result', 'do': [{'$y': '$y', '+': 1}]}",
                    "{'print': '$y'}"
                    );
            assertCommand(index, 
                    "[nt:unstructured] as [a] /* nodeType Filter(query=" + 
                            "explain select [jcr:path], [jcr:score], * from [nt:unstructured] as a " + 
                            "where [x] = 1 and isdescendantnode(a, '/') /* xpath: " + 
                            "/jcr:root//element(*, nt:unstructured)[@x=1] */, path=//*, " + 
                            "property=[x=[1]]) where ([a].[x] = 1) and (isdescendantnode([a], [/])) */",
                    "{'setProperty': '/oak:index/test/type', 'value': 'disabled'}",
                    "{'session':'save'}",
                    "{'xpath':'explain /jcr:root//element(*, nt:unstructured)[@x=1]'}"
                    );
            assertCommand(index, 
                    "[nt:unstructured] as [a] /* traverse '*' " + 
                            "where [a].[x] = 1 */",
                    "{'removeNode': '/oak:index/nodetype'}",
                    "{'session':'save'}",
                    "{'sql':'explain select * from [nt:unstructured] as [a] where [x]=1'}"
                    );
            assertCommand(index, 
                    "['/foo': {\n" +
                    "  'jcr:primaryType': 'nt:unstructured', '{Long}x': '1', 'y': {}, 'test': {}\n" +
                    "}]",
                    "{'xpath':'/jcr:root/foo', 'depth':2}"
                    );
            index.session.logout();
        }
    }
    
    void assertCommand(String expected, String... commands) throws Exception {
        assertCommand(new IndexCommand(), expected, commands);
    }

    void assertCommand(IndexCommand index, String expected, String... commands) throws Exception {
        ByteArrayOutputStream w = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(w, false, "UTF-8");
        index.output = out;
        for(String c : commands) {
            index.execute(c.replace('\'', '"').replace('`', '\''));
        }
        String got = new String(w.toByteArray());
        assertEquals(expected, got.trim().replace('"', '\''));
    }
    
}
