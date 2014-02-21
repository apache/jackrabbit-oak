/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import static org.apache.jackrabbit.oak.plugins.document.Node.Children;
import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.concurrent.Callable;

import com.mongodb.BasicDBObject;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

/**
 * Simple test to measure how much memory a certain object uses.
 */
public class MeasureMemory {
    
    static final boolean TRACE = false;
    
    static final int TEST_COUNT = 10000;
    static final int OVERHEAD = 24;

    static final DocumentNodeStore STORE = new DocumentMK.Builder()
            .setAsyncDelay(0).getNodeStore();

    @Test
    public void overhead() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                return new Object[]{"", OVERHEAD};
            }
        });
    }
    
    @Test
    public void node() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                Node n = generateNode(5);
                return new Object[]{n, n.getMemory() + OVERHEAD};
            }
        });
    }
    
    @Test
    public void nodeWithoutProperties() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                Node n = generateNode(0);
                return new Object[]{n, n.getMemory() + OVERHEAD};
            }
        });
    }

    @Test
    public void basicObject() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                BasicDBObject n = generateBasicObject(15);
                return new Object[]{n, Utils.estimateMemoryUsage(n) + OVERHEAD};
            }
        });
    }

    @Test
    public void basicObjectWithoutProperties() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                BasicDBObject n = generateBasicObject(0);
                return new Object[]{n, Utils.estimateMemoryUsage(n) + OVERHEAD};
            }
        });
    }
    
    @Test
    public void nodeChildManyChildren() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                Children n = generateNodeChild(100);
                return new Object[]{n, n.getMemory() + OVERHEAD};
            }
        });
    }

    @Test
    public void nodeChild() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                Children n = generateNodeChild(5);
                return new Object[]{n, n.getMemory() + OVERHEAD};
            }
        });
    }

    @Test
    public void nodeChildWithoutChildren() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                Children n = generateNodeChild(0);
                return new Object[]{n, n.getMemory() + OVERHEAD};
            }
        });
    }
    
    private static void measureMemory(Callable<Object[]> c) throws Exception {
        LinkedList<Object> list = new LinkedList<Object>();
        long base = getMemoryUsed();
        long mem = 0;
        for (int i = 0; i < TEST_COUNT; i++) {
            Object[] om = c.call();
            list.add(om[0]);
            mem += (Integer) om[1];
        }
        long used = getMemoryUsed() - base;
        int estimation = (int) (100 * mem / used);
        String message = 
                new Error().getStackTrace()[1].getMethodName() + "\n" +
                "used: " + used + " calculated: " + mem + "\n" +
                "estimation is " + estimation + "%\n";
        if (TRACE) {
            System.out.println(message);
        }
        if (estimation < 80 || estimation > 160) {
            fail(message);
        }
        // need to keep the reference until here, otherwise
        // the list might be garbage collected too early
        list.clear();
    }
    
    static Node generateNode(int propertyCount) {
        Node n = new DocumentNodeState(STORE, new String("/hello/world"),
                new Revision(1, 2, 3));
        for (int i = 0; i < propertyCount; i++) {
            n.setProperty("property" + i, "\"values " + i + "\"");
        }
        n.setLastRevision(new Revision(1, 2, 3));
        return n;
    }

    static BasicDBObject generateBasicObject(int propertyCount) {
        BasicDBObject n = new BasicDBObject(new String("_id"), new String(
                "/hello/world"));
        for (int i = 0; i < propertyCount; i++) {
            n.append("property" + i, "values " + i);
        }
        return n;
    }

    static Children generateNodeChild(int childCount) {
        Children n = new Children();
        for (int i = 0; i < childCount; i++) {
            n.children.add("child" + i);
        }
        return n;
    }
    
    private static long getMemoryUsed() {
        for (int i = 0; i < 10; i++) {
            System.gc();
        }
        return Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory();
    }
    
}
