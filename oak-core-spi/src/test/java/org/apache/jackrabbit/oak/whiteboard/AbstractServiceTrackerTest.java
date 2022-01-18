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
package org.apache.jackrabbit.oak.whiteboard;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static javax.jcr.Repository.REP_VENDOR_DESC;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class AbstractServiceTrackerTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Test
    public void testTrack() {
        TestServiceTracker tracker = new TestServiceTracker();
        try {
            tracker.start(new OsgiWhiteboard(context.bundleContext()));
            tracker.assertSize(0);

            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.emptyMap());
            tracker.assertSize(1);

            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.singletonMap(REP_VENDOR_DESC, "value"));
            tracker.assertSize(2);

            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.singletonMap("differentProperty", "Oak"));
            tracker.assertSize(3);

            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.singletonMap(REP_VENDOR_DESC, "Oak"));
            tracker.assertSize(4);

            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.singletonMap(REP_VENDOR_DESC, "Oak"));
            tracker.assertSize(5);
        } finally {
            tracker.stop();
            tracker.assertSize(0);
        }
    }

    @Test
    public void testTrackWithProperties() {
        TestServiceTracker tracker = new TestServiceTracker(Collections.singletonMap(REP_VENDOR_DESC, "Oak"));
        try {
            tracker.start(new OsgiWhiteboard(context.bundleContext()));
            tracker.assertSize(0);

            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.emptyMap());
            tracker.assertSize(0);
            
            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.singletonMap(REP_VENDOR_DESC, "mismatch"));
            tracker.assertSize(0);

            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.singletonMap("differentProperty", "Oak"));
            tracker.assertSize(0);

            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.singletonMap(REP_VENDOR_DESC, "Oak"));
            tracker.assertSize(1);

            context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.singletonMap(REP_VENDOR_DESC, "Oak"));
            tracker.assertSize(2);
        } finally {
            tracker.stop();
            tracker.assertSize(0);
        }
    }
    
    @Test(expected = IllegalStateException.class) 
    public void testStartTwice() {
        TestServiceTracker tracker = new TestServiceTracker();
        tracker.start(new OsgiWhiteboard(context.bundleContext()));
        tracker.start(new OsgiWhiteboard(context.bundleContext()));
    }

    @Test()
    public void testStartStart() {
        TestServiceTracker tracker = new TestServiceTracker();
        tracker.stop();
        tracker.start(new OsgiWhiteboard(context.bundleContext()));

        context.registerService(ContentRepository.class, mock(ContentRepository.class), Collections.emptyMap());
        tracker.assertSize(1);
    }
    
    private static final class TestServiceTracker extends AbstractServiceTracker<ContentRepository> {
        protected TestServiceTracker() {
            super(ContentRepository.class);
        }

        protected TestServiceTracker(@NotNull Map<String, String> filterProperties) {
            super(ContentRepository.class, filterProperties);
        }
        
        void assertSize(int expectedSize) {
            assertEquals(expectedSize, getServices().size());
        } 
    }
    
}