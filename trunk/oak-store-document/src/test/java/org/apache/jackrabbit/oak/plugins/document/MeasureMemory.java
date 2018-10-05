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

import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeState.Children;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.util.RevisionsKey;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
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

    static final Blob BLOB;
    static final String BLOB_VALUE;

    static {
        try {
            BLOB = STORE.createBlob(new RandomStream(1024 * 1024, 42));
            NodeBuilder builder = STORE.getRoot().builder();
            builder.child("binary").setProperty(new BinaryPropertyState("b", BLOB));
            STORE.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            NodeState n = STORE.getRoot().getChildNode("binary");
            BLOB_VALUE = ((DocumentNodeState) n).getPropertyAsString("b");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

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
                DocumentNodeState n = generateNode(5);
                return new Object[]{n, n.getMemory() + OVERHEAD};
            }
        });
    }

    @Test
    public void nodeWithoutProperties() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                DocumentNodeState n = generateNode(0);
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

    @Test
    public void nodeWithBinaryProperty() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                DocumentNodeState n = generateNodeWithBinaryProperties(3);
                return new Object[]{n, n.getMemory() + OVERHEAD};
            }
        });
    }

    @Test
    public void revisionsKey() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() {
                RevisionsKey k = new RevisionsKey(
                        new RevisionVector(Revision.newRevision(0)),
                        new RevisionVector(Revision.newRevision(0)));
                return new Object[]{k, k.getMemory() + OVERHEAD};
            }
        });
    }

    @Test
    public void revisionVector() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() throws Exception {
                RevisionVector rv = new RevisionVector(
                        Revision.newRevision(0),
                        Revision.newRevision(1),
                        Revision.newRevision(2),
                        Revision.newRevision(3));
                return new Object[]{rv, rv.getMemory() + OVERHEAD};
            }
        });
    }

    @Test
    public void revisionVectorSingle() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() throws Exception {
                RevisionVector rv = new RevisionVector(Revision.newRevision(0));
                return new Object[]{rv, rv.getMemory() + OVERHEAD};
            }
        });
    }

    @Test
    public void revision() throws Exception {
        measureMemory(new Callable<Object[]>() {
            @Override
            public Object[] call() throws Exception {
                Revision r = Revision.newRevision(0);
                return new Object[]{r, r.getMemory() + OVERHEAD};
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

    static DocumentNodeState generateNode(int propertyCount) {
        return generateNode(propertyCount, Collections.<PropertyState>emptyList());
    }

    static DocumentNodeState generateNode(int propertyCount, List<PropertyState> extraProps) {
        List<PropertyState> props = Lists.newArrayList();
        props.addAll(extraProps);
        for (int i = 0; i < propertyCount; i++) {
            String key = "property" + i;
            props.add(STORE.createPropertyState(key, "\"values " + i + "\""));
        }
        return new DocumentNodeState(STORE, new String("/hello/world"),
                new RevisionVector(new Revision(1, 2, 3)), props, false, new RevisionVector(new Revision(1, 2, 3)));
    }

    static DocumentNodeState generateNodeWithBinaryProperties(int propertyCount) {
        List<PropertyState> props = Lists.newArrayList();
        for (int i = 0; i < propertyCount; i++) {
            props.add(STORE.createPropertyState("binary" + i, new String(BLOB_VALUE)));
        }
        return generateNode(0, props);
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
