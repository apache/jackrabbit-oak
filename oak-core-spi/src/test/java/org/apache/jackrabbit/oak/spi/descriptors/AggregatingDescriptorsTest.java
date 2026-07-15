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
package org.apache.jackrabbit.oak.spi.descriptors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.junit.Test;

public class AggregatingDescriptorsTest {

    static class DescriptorEntry {
        private final String key;
        private final Value value;
        private final Value[] values;
        private final boolean singleValued;
        private final boolean standard;

        DescriptorEntry(String key, Value value, Value[] values, boolean singleValued,
                boolean standard) {
            this.key = key;
            this.value = value;
            this.values = values;
            this.singleValued = singleValued;
            this.standard = standard;
        }

        static DescriptorEntry fromKey(String key,
                Descriptors descriptors) {
            if (descriptors.isSingleValueDescriptor(key)) {
                return newSingleValuedEntry(key, descriptors.getValue(key),
                        descriptors.isStandardDescriptor(key));
            } else {
                return newMultiValuedEntry(key, descriptors.getValues(key),
                        descriptors.isStandardDescriptor(key));
            }
        }

        private static DescriptorEntry newMultiValuedEntry(String key,
                Value[] values, boolean standardDescriptor) {
            return new DescriptorEntry(key, null, values, false, standardDescriptor);
        }

        private static DescriptorEntry newSingleValuedEntry(String key,
                Value value, boolean standardDescriptor) {
            return new DescriptorEntry(key, value, null, true, standardDescriptor);
        }

    }

    class MyTracker<T> implements Tracker<T> {

        List<T> services;

        public void setServices(List<T> services) {
            this.services = services;
        }

        public void addService(T service) {
            if (services == null) {
                services = new LinkedList<T>();
            }
            services.add(service);
        }

        @Override
        public List<T> getServices() {
            return services;
        }

        @Override
        public void stop() {
            // no-op
        }

    }

    private MyTracker<Descriptors> createTracker() {
        return new MyTracker<Descriptors>();
    }

    private void assertEmpty(AggregatingDescriptors aggregator) {
        assertFalse(aggregator.isSingleValueDescriptor("foo"));
        assertFalse(aggregator.isStandardDescriptor("foo"));
        assertNull(aggregator.getValue("foo"));
        assertNull(aggregator.getValues("foo"));
        String[] keys = aggregator.getKeys();
        assertNotNull(keys);
        assertEquals(0, keys.length);
    }

    @Test
    public void testNullServices() throws Exception {
        try {
            new AggregatingDescriptors(null);
            fail("should complain");
        } catch (IllegalArgumentException iae) {
            // ok
        }
        final MyTracker<Descriptors> tracker = createTracker();
        AggregatingDescriptors aggregator = new AggregatingDescriptors(tracker);
        assertEmpty(aggregator);
    }

    @Test
    public void testEmptyServices() throws Exception {
        final MyTracker<Descriptors> tracker = createTracker();
        AggregatingDescriptors aggregator = new AggregatingDescriptors(tracker);
        tracker.setServices(new LinkedList<Descriptors>());
        assertEmpty(aggregator);
    }

    private void assertMatches(AggregatingDescriptors aggregator, int expectedEntryCount,
            GenericDescriptors... descriptors) {
        // prepare the expectedEntries map
        final Map<String, DescriptorEntry> expectedEntries = new HashMap<String, AggregatingDescriptorsTest.DescriptorEntry>();
        for (int i = 0; i < descriptors.length; i++) {
            final String[] keys = descriptors[i].getKeys();
            for (int j = 0; j < keys.length; j++) {
                final DescriptorEntry entry = DescriptorEntry.fromKey(keys[j], descriptors[i]);
                // implements overwriting: eg descriptors[1] values overwrite descriptors[0] values
                // (in terms of the AggregatingDescriptors it is the opposite: the service
                // that is enlisted first always wins - with the idea that later added
                // services should not overwrite earlier ones - lowest startorder wins)
                expectedEntries.put(keys[j], entry);
            }
        }
        
        assertEquals(expectedEntryCount, expectedEntries.size());
        
        // now go through the resulting expectedEntries and match them
        // with the aggregator one
        final Collection<DescriptorEntry> entries = expectedEntries.values();
        for (Iterator<DescriptorEntry> it = entries.iterator(); it.hasNext();) {
            DescriptorEntry entry = it.next();
            assertEquals(entry.standard, aggregator.isStandardDescriptor(entry.key));
            if (entry.singleValued) {
                assertTrue(aggregator.isSingleValueDescriptor(entry.key));
                Value expectedValue = entry.value;
                Value actualValue = aggregator.getValue(entry.key);
                assertTrue(expectedValue.equals(actualValue));
            } else {
                assertFalse(aggregator.isSingleValueDescriptor(entry.key));
                Value[] expectedValues = entry.values;
                Value[] actualValues = aggregator.getValues(entry.key);
                assertEquals(expectedValues.length, actualValues.length);
                for(int i=0; i<expectedValues.length; i++) {
                    assertEquals(expectedValues[i], actualValues[i]);
                }
            }
        }
        assertEquals(expectedEntryCount, aggregator.getKeys().length);
    }

    @Test
    public void testInitialDescriptors() throws Exception {
        final ValueFactory valueFactory = new SimpleValueFactory();
        final MyTracker<Descriptors> tracker = createTracker();
        final GenericDescriptors input = new GenericDescriptors();
        input.put("a", valueFactory.createValue("b"), true, false);
        input.put("b", valueFactory.createValue("c"), true, true);
        tracker.addService(input);
        AggregatingDescriptors aggregator = new AggregatingDescriptors(tracker);
        assertMatches(aggregator, 2, input);
    }

    @Test
    public void testLaterAddedDescriptors() throws Exception {
        final ValueFactory valueFactory = new SimpleValueFactory();
        final MyTracker<Descriptors> tracker = createTracker();
        AggregatingDescriptors aggregator = new AggregatingDescriptors(tracker);
        assertMatches(aggregator, 0);
        final GenericDescriptors input1 = new GenericDescriptors();
        input1.put("a", valueFactory.createValue("b"), true, false);
        input1.put("b", valueFactory.createValue("c"), true, true);
        tracker.addService(input1);
        assertMatches(aggregator, 2, input1);
        final GenericDescriptors input2 = new GenericDescriptors();
        input2.put("b", valueFactory.createValue("c2"), true, true);
        input2.put("c", valueFactory.createValue("d"), true, true);
        tracker.addService(input2);
        assertMatches(aggregator, 3, input2, input1);
    }

}
