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
package org.apache.jackrabbit.mongomk.prototype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mongomk.prototype.DocumentStore.Collection;
import org.apache.jackrabbit.mongomk.prototype.Node.Children;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * A set of simple tests.
 */
public class SimpleTest {

    @Test
    public void test() {
        MongoMK mk = new MongoMK();
        mk.dispose();
    }
    
    @Test
    public void revision() {
        for (int i=0; i<100; i++) {
            Revision r = Revision.newRevision(i);
            // System.out.println(r);
            Revision r2 = Revision.fromString(r.toString());
            assertEquals(r.toString(), r2.toString());
            assertEquals(r.hashCode(), r2.hashCode());
            assertTrue(r.equals(r2));
        }
    }
    
    @Test
    public void addNodeGetNode() {
        MongoMK mk = new MongoMK();
        Revision rev = mk.newRevision();
        Node n = new Node("/", rev);
        n.setProperty("name", "Hello");
        UpdateOp op = n.asOperation(true);
        DocumentStore s = mk.getDocumentStore();
        s.create(Collection.NODES, Lists.newArrayList(op));
        Node n2 = mk.getNode("/", rev);
        assertEquals("Hello", n2.getProperty("name"));
        mk.dispose();
    }

    @Test
    public void commit() {
        MongoMK mk = new MongoMK();

        String rev;
        rev = mk.commit("/", "+\"test\":{\"name\": \"Hello\"}", null, null);
        String test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"name\":\"Hello\",\":childNodeCount\":0}", test);
        
        rev = mk.commit("/test", "+\"a\":{\"name\": \"World\"}", null, null);
        rev = mk.commit("/test", "+\"b\":{\"name\": \"!\"}", null, null);
        test = mk.getNodes("/test", rev, 0, 0, Integer.MAX_VALUE, null);
        Children c;
        c = mk.readChildren("/", 
                Revision.fromString(rev), Integer.MAX_VALUE);
        assertEquals("/: [/test]", c.toString());
        c = mk.readChildren("/test", 
                Revision.fromString(rev), Integer.MAX_VALUE);
        assertEquals("/test: [/test/a, /test/b]", c.toString());

        rev = mk.commit("", "^\"/test\":1", null, null);
        test = mk.getNodes("/", rev, 0, 0, Integer.MAX_VALUE, null);
        assertEquals("{\"test\":1,\"test\":{},\":childNodeCount\":1}", test);

        System.out.println(test);
        mk.dispose();
    }
    
    // TODO run Damians tests
    
}
