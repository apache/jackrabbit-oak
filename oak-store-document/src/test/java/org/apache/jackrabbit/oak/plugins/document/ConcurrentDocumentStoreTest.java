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
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentDocumentStoreTest extends AbstractDocumentStoreTest {

    static final Logger LOG = LoggerFactory.getLogger(ConcurrentDocumentStoreTest.class);

    public ConcurrentDocumentStoreTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Test
    public void testConcurrentUpdate() throws Exception {
        int workers = 20; // TODO: this test is going to fail if the number of
                          // workers exceeds the number of retries done by
                          // RDBDocumentStore
        String id = this.getClass().getName() + ".testConcurrentUpdate";
        UpdateOp up = new UpdateOp(id, true);
        up.set("thread", Thread.currentThread().getName());
        up.set("counter", 0L);
        ds.create(Collection.NODES, Collections.singletonList(up));
        super.removeMe.add(id);
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
        List<Thread> worker = new ArrayList<Thread>();
        for (int i = 0; i < workers; i++) {
            worker.add(new Thread(new Worker(id, false, exceptions)));
        }
        for (Thread t : worker) {
            t.start();
        }
        for (Thread t : worker) {
            t.join();
        }
        for (Exception e : exceptions) {
            fail(e.toString());
        }
        Document d = ds.find(Collection.NODES, id);
        String val = d.get("counter").toString();
        org.junit.Assert.assertEquals("counter property not updated as expected", Integer.toString(workers), val);
    }

    @Test
    public void testConcurrentCreateOrUpdate() throws Exception {
        int workers = 8; // TODO: this test is going to fail if the number of
                         // workers exceeds the number of retries done by
                         // RDBDocumentStore
        String id = this.getClass().getName() + ".testConcurrentCreateOrUpdate";
        super.removeMe.add(id);
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
        List<Thread> worker = new ArrayList<Thread>();
        for (int i = 0; i < workers; i++) {
            worker.add(new Thread(new Worker(id, true, exceptions)));
        }
        for (Thread t : worker) {
            t.start();
        }
        for (Thread t : worker) {
            t.join();
        }
        for (Exception e : exceptions) {
            fail(e.toString());
        }
        Document d = ds.find(Collection.NODES, id);
        String val = d.get("counter").toString();
        org.junit.Assert.assertEquals("counter property not updated as expected", Integer.toString(workers), val);
    }

    private final class Worker implements Runnable {

        private final String id;
        private final boolean create;
        private final List<Exception> exceptions;

        Worker(String id, boolean create, List<Exception> exceptions) {
            this.id = id;
            this.create = create;
            this.exceptions = exceptions;
        }

        @Override
        public void run() {
            try {
                UpdateOp up = new UpdateOp(id, true);
                up.set("thread", Thread.currentThread().getName());
                up.increment("counter", 1L);
                if (create) {
                    ds.createOrUpdate(Collection.NODES, up);
                } else {
                    Document d = ds.findAndUpdate(Collection.NODES, up);
                    assertTrue(d.isSealed());
                }
            } catch (Exception ex) {
                LOG.error("trying to create/update", ex);
                exceptions.add(ex);
            }
        }
    }
}
