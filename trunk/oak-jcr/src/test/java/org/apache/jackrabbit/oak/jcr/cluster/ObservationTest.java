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
package org.apache.jackrabbit.oak.jcr.cluster;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.api.observation.JackrabbitEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static java.util.Collections.synchronizedSet;
import static org.junit.Assert.assertTrue;

/**
 * Test for external events from another cluster node.
 */
@Ignore("This is a rather long running test and therefore disabled. See " +
        "DocumentNodeStoreTest.diffExternalChanges() in oak-core for a unit test")
public class ObservationTest extends AbstractClusterTest {

    private ScheduledExecutorService executor;

    @Override
    protected void prepareTestData(Session s) throws RepositoryException {
        if (s.itemExists("/test")) {
            s.getNode("/test").remove();
        }
        s.getRootNode().addNode("test", "oak:Unstructured");
        s.save();
    }

    @Before
    public void before() throws Exception {
        executor = Executors.newScheduledThreadPool(4);
    }

    @After
    public void after() {
        executor.shutdown();
    }

    @Test
    public void externalEvents() throws Throwable {
        final Set<String> externallyAdded = synchronizedSet(new LinkedHashSet<String>());
        final List<Throwable> exceptions = Lists.newArrayList();
        ObservationManager obsMgr = s1.getWorkspace().getObservationManager();
        final AtomicLong localEvents = new AtomicLong();
        final AtomicLong externalEvents = new AtomicLong();
        EventListener listener = new EventListener() {
            @Override
            public void onEvent(EventIterator events) {
                try {
                    Set<String> paths = Sets.newHashSet();
                    while (events.hasNext()) {
                        Event event = events.nextEvent();
                        String external = "";
                        AtomicLong counter = localEvents;
                        if (event instanceof JackrabbitEvent) {
                           if (((JackrabbitEvent) event).isExternal()) {
                               external = " (external)";
                               counter = externalEvents;
                               paths.add(event.getPath());
                           }
                        }
                        System.out.println(event.getPath() + external);
                        counter.incrementAndGet();
                    }
                    while (!paths.isEmpty()) {
                        Iterator<String> it = externallyAdded.iterator();
                        String p = it.next();
                        assertTrue("missing event for " + p, paths.remove(p));
                        it.remove();
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                }
            }
        };
        obsMgr.addEventListener(listener, Event.NODE_ADDED, "/",
                true, null, null, false);

        Future f1 = executor.submit(new Worker(s1, exceptions, new HashSet<String>()));
        Future f2 = executor.submit(new Worker(s2, exceptions, externallyAdded));

        f1.get();
        f2.get();

        Thread.sleep(10 * 1000);

        System.out.println("local events: " + localEvents.get());
        System.out.println("external events: " + externalEvents.get());

        for (Throwable t : exceptions) {
            throw t;
        }
    }

    class Worker implements Runnable {

        private Session s;
        private List<Throwable> exceptions;
        private Set<String> paths;

        Worker(Session s, List<Throwable> exceptions, Set<String> paths) {
            this.s = s;
            this.exceptions = exceptions;
            this.paths = paths;
        }

        @Override
        public void run() {
            try {
                Node test = s.getNode("/test");
                for (int i = 0; i < 500 && exceptions.isEmpty(); i++) {
                    Node n = test.addNode(UUID.randomUUID().toString());
                    paths.add(n.getPath());
                    s.save();
                    Thread.sleep((long) (Math.random() * 100));
                }
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
    }
}
