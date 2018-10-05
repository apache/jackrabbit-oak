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

package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

public class ObserverTrackerTest {
    private final Set<Observer> observers = newHashSet();

    private final Observable observable = new Observable() {
        @Override
        public Closeable addObserver(final Observer observer) {
            observers.add(observer);
            return new Closeable() {
                @Override
                public void close() {
                    if (!observers.remove(observer)) {
                        fail("Already closed");
                    }
                }
            };
        }
    };

    private final ObserverTracker tracker = new ObserverTracker(observable);

    private final ServiceReference ref1 = mock(ServiceReference.class);
    private final Observer observer1 = mock(Observer.class);
    private final ServiceReference ref2 = mock(ServiceReference.class);
    private final Observer observer2 = mock(Observer.class);

    @Before
    public void setup() {
        BundleContext bundleContext = mock(BundleContext.class);
        when(bundleContext.getService(ref1)).thenReturn(observer1);
        when(bundleContext.getService(ref2)).thenReturn(observer2);
        tracker.start(bundleContext);
    }

    @After
    public void tearDown() {
        tracker.stop();
    }

    @Test
    public void registerUnregister() {
        tracker.addingService(ref1);
        assertEquals(ImmutableSet.of(observer1), observers);

        tracker.addingService(ref2);
        assertEquals(ImmutableSet.of(observer1, observer2), observers);

        tracker.removedService(ref1, null);
        assertEquals(ImmutableSet.of(observer2), observers);

        tracker.removedService(ref2, null);
        assertTrue(observers.isEmpty());
    }

    @Test
    public void registerTwice() {
        tracker.addingService(ref1);
        assertEquals(ImmutableSet.of(observer1), observers);

        // Adding an already added service should have no effect
        tracker.addingService(ref1);
        assertEquals(ImmutableSet.of(observer1), observers);
    }

    @Test
    public void unregisterWhenEmpty() {
        tracker.removedService(ref1, null);
        assertTrue(observers.isEmpty());
    }

    @Test
    public void unregisterTwice() {
        tracker.addingService(ref1);
        assertEquals(ImmutableSet.of(observer1), observers);

        tracker.removedService(ref1, null);
        assertTrue(observers.isEmpty());

        // Removing a removed service should have no effect
        tracker.removedService(ref1, null);
        assertTrue(observers.isEmpty());
    }

}
