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

package org.apache.jackrabbit.oak.plugins.mongomk.cache;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Ordering;
import org.apache.jackrabbit.oak.plugins.mongomk.DocumentStore;
import org.apache.jackrabbit.oak.plugins.mongomk.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.mongomk.NodeDocument;
import org.apache.jackrabbit.oak.plugins.mongomk.Revision;
import org.apache.jackrabbit.oak.plugins.mongomk.StableRevisionComparator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SerializerTest {
    private DocumentStore store = new MemoryDocumentStore();

    @Test
    public void revisionSerialization() {
        Revision r = new Revision(System.currentTimeMillis(), 1, 5);
        assertEquals(r, deserialize(r));

        r = new Revision(System.currentTimeMillis(), 1, 5, true);
        assertEquals(r, deserialize(r));
    }

    @Test
    public void nodeDocSerialization() {
        long time = System.currentTimeMillis();
        NodeDocument doc = new NodeDocument(store,time);
        doc.seal();
        checkSame(doc, (NodeDocument) deserialize(doc));

        doc = new NodeDocument(store,time);
        doc.put("_id","b1");
        doc.put("a2","b2");
        doc.seal();
        checkSame(doc, (NodeDocument) deserialize(doc));

        doc = new NodeDocument(store,time);
        doc.put("_id","b1");
        doc.put("a2",createRevisionMap());
        doc.put("a3",createRevisionMap());
        doc.seal();

        NodeDocument deserDoc = (NodeDocument) deserialize(doc);
        checkSame(doc, deserDoc);

        //Assert that revision keys are sorted
        NavigableMap<Revision,Object> values = (NavigableMap<Revision, Object>) deserDoc.get("a2");
        assertTrue(Ordering.from(StableRevisionComparator.REVERSE).isOrdered(values.keySet()));
    }

    private Object deserialize(Object data){
        Kryo k = KryoFactory.createInstance(store);
        Output o = new Output(1024*1024);
        k.writeObject(o,data);
        o.close();

        Input input = new Input(o.getBuffer(), 0, o.position());
        Object result = k.readObject(input,data.getClass());
        input.close();
        System.out.printf("Size %d %s %n",o.position(), data);
        return result;
    }

    private static Map<Revision,Object> createRevisionMap(){
        Map<Revision,Object> map = new TreeMap<Revision, Object>(StableRevisionComparator.REVERSE);
        for(int i = 0; i < 10; i++){
            map.put(new Revision(System.currentTimeMillis() + i, 0, 2),"foo"+i);
        }
        return map;
    }

    private static void checkSame(NodeDocument d1, NodeDocument d2){
        assertEquals(d1.getCreated(), d2.getCreated());
        assertEquals(d1.keySet(), d2.keySet());
        for(String key : d1.keySet()){
            assertEquals(d1.get(key), d2.get(key));
        }
    }
}
