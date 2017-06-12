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
package org.apache.jackrabbit.oak.spi.whiteboard;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class DefaultWhiteboardTest {

    private Whiteboard whiteboard;

    @Before
    public void createWhiteboard() {
        whiteboard = new DefaultWhiteboard();
    }

    @Test
    public void filteredTracker() {
        whiteboard.register(Service1.class, new Service1("s1"), ImmutableMap.of());
        whiteboard.register(Service2.class, new Service2("s2"), ImmutableMap.of("role", "myrole"));
        whiteboard.register(Service3.class, new Service3("s3_1"), ImmutableMap.of());
        whiteboard.register(Service3.class, new Service3("s3_2"), ImmutableMap.of("role", "myrole"));
        whiteboard.register(Service3.class, new Service3("s3_3"), ImmutableMap.of("role", "myotherrole", "id", 1024));

        assertEquals(of("s1"), track(Service1.class));
        assertEquals(of("s1"), track(Service1.class, singletonMap("role", null)));
        assertEquals(of(), track(Service1.class, ImmutableMap.of("role", "myrole")));

        assertEquals(of("s2"), track(Service2.class));
        assertEquals(of(), track(Service2.class, singletonMap("role", null)));
        assertEquals(of("s2"), track(Service2.class, ImmutableMap.of("role", "myrole")));

        assertEquals(of("s3_1", "s3_2", "s3_3"), track(Service3.class));
        assertEquals(of("s3_1"), track(Service3.class, singletonMap("role", null)));
        assertEquals(of("s3_2"), track(Service3.class, ImmutableMap.of("role", "myrole")));
        assertEquals(of("s3_3"), track(Service3.class, ImmutableMap.of("role", "myotherrole")));
        assertEquals(of("s3_3"), track(Service3.class, ImmutableMap.of("role", "myotherrole", "id", "1024")));
        assertEquals(of("s3_3"), track(Service3.class, ImmutableMap.of("id", "1024")));
        assertEquals(of(), track(Service3.class, ImmutableMap.of("id", "2048")));
    }

    @Test
    public void sameServiceRegisteredAgain() {
        Service1 s1 = new Service1("s1");

        whiteboard.register(Service1.class, s1, ImmutableMap.of());
        whiteboard.register(Service1.class, s1, ImmutableMap.of());
        whiteboard.register(Service1.class, s1, ImmutableMap.of());

        assertEquals(of("s1"), track(Service1.class));
    }

    @Test
    public void unregister() {
        Registration r1 = whiteboard.register(Service1.class, new Service1("s1"), ImmutableMap.of());
        Registration r2 = whiteboard.register(Service2.class, new Service2("s2"), ImmutableMap.of("role", "myrole"));
        Registration r3_1 = whiteboard.register(Service3.class, new Service3("s3_1"), ImmutableMap.of());
        Registration r3_2 = whiteboard.register(Service3.class, new Service3("s3_2"), ImmutableMap.of("role", "myrole"));
        Registration r3_3 = whiteboard.register(Service3.class, new Service3("s3_3"), ImmutableMap.of("role", "myotherrole", "id", 1024));

        assertEquals(of("s1"), track(Service1.class));
        r1.unregister();
        assertEquals(of(), track(Service1.class));

        assertEquals(of("s2"), track(Service2.class));
        r2.unregister();
        assertEquals(of(), track(Service2.class));

        assertEquals(of("s3_1", "s3_2", "s3_3"), track(Service3.class));
        r3_1.unregister();
        assertEquals(of("s3_2", "s3_3"), track(Service3.class));
        r3_2.unregister();
        assertEquals(of("s3_3"), track(Service3.class));
        r3_3.unregister();
        assertEquals(of(), track(Service3.class));
    }

    private <T extends Service> Set<String> track(Class<T> clazz) {
        return track(clazz, null);
    }

    private <T extends Service> Set<String> track(Class<T> clazz, Map<String, String> properties) {
        final Tracker<T> tracker;
        if (properties == null) {
            tracker = whiteboard.track(clazz);
        } else {
            tracker = whiteboard.track(clazz, properties);
        }
        try {
            return tracker.getServices().stream().map(Service::getId).collect(toSet());
        } finally {
            tracker.stop();
        }
    }

    public abstract static class Service {
        private final String id;

        private Service(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    private final static class Service1 extends Service {
        private Service1(String id) {
            super(id);
        }
    }

    private final static class Service2 extends Service {
        private Service2(String id) {
            super(id);
        }
    }

    private final static class Service3 extends Service {
        private Service3(String id) {
            super(id);
        }
    }
}
