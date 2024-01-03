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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;
import org.junit.Test;

public class IndexedPropertyTest {

    @Test
    public void test() {
        IndexedProperty p = IndexedProperty.create("nt:file", "jcr:data/x");
        assertEquals("(nt:file) jcr:data/x", p.toString());
        assertEquals("x", p.getPropertyName());

        // wrong path
        NodeData n = new NodeData(Arrays.asList(), 
                Arrays.asList(new NodeProperty("x", ValueType.STRING, "1")));
        assertFalse(p.matches("x", n));

        // wrong node name
        n = new NodeData(Arrays.asList("a"), 
                Arrays.asList(new NodeProperty("x", ValueType.STRING, "1")));
        assertFalse(p.matches("x", n));
        
        // parent has wrong type
        n = new NodeData(Arrays.asList("jcr:data"), 
                Arrays.asList(new NodeProperty("x", ValueType.STRING, "1")));
        NodeData parent = new NodeData(Arrays.asList(), 
                Arrays.asList(new NodeProperty("jcr:primaryType", ValueType.NAME, "nt:folder")));
        n.setParent(parent);
        assertFalse(p.matches("x", n));
        
        // all good
        n = new NodeData(Arrays.asList("jcr:data"), 
                Arrays.asList(new NodeProperty("x", ValueType.STRING, "1")));
        parent = new NodeData(Arrays.asList(), 
                Arrays.asList(new NodeProperty("jcr:primaryType", ValueType.NAME, "nt:file")));
        n.setParent(parent);
        assertTrue(p.matches("x", n));
        
        // wrong property name
        assertFalse(p.matches("y", n));

    }
}
