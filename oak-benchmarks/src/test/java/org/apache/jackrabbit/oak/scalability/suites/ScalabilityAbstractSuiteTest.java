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

package org.apache.jackrabbit.oak.scalability.suites;

import org.apache.jackrabbit.oak.scalability.ScalabilitySuite;
import org.apache.jackrabbit.oak.scalability.benchmarks.ScalabilityBenchmark;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ScalabilityAbstractSuiteTest {

    private String originalIncrementsProperty;

    @Before
    public void saveOriginalProperty() {
        originalIncrementsProperty = System.getProperty("increments");
    }

    @After
    public void restoreOriginalProperty() throws Exception {
        if (originalIncrementsProperty == null) {
            System.clearProperty("increments");
        } else {
            System.setProperty("increments", originalIncrementsProperty);
        }
    }

    private List<String> getIncrementsList(String value) {
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(java.util.stream.Collectors.toList());
    }

    @Test
    public void testDefaultIncrementsValue() throws Exception {
        System.clearProperty("increments");

        // Create a new instance to reinitialize static fields
        new TestScalabilityAbstractSuite();

        Field incrementsField = ScalabilityAbstractSuite.class.getDeclaredField("INCREMENTS");
        incrementsField.setAccessible(true);
        List<String> increments = (List<String>) incrementsField.get(null);

        // Clear and populate the list with new values
        increments.clear();
        increments.addAll(getIncrementsList(System.getProperty("increments", "1,2,5")));


        assertEquals(3, increments.size());
        assertEquals("1", increments.get(0));
        assertEquals("2", increments.get(1));
        assertEquals("5", increments.get(2));
    }

    @Test
    public void testCustomIncrementsValue() throws Exception {
        System.setProperty("increments", "10,20,30,40");

        // Create a new instance to reinitialize static fields
        new TestScalabilityAbstractSuite();

        Field incrementsField = ScalabilityAbstractSuite.class.getDeclaredField("INCREMENTS");
        incrementsField.setAccessible(true);
        List<String> increments = (List<String>) incrementsField.get(null);
        // Clear and populate the list with new values
        increments.clear();
        increments.addAll(getIncrementsList(System.getProperty("increments", "1,2,5")));

        assertEquals(4, increments.size());
        assertEquals("10", increments.get(0));
        assertEquals("20", increments.get(1));
        assertEquals("30", increments.get(2));
        assertEquals("40", increments.get(3));
    }

    @Test
    public void testWhitespaceTrimmingInIncrements() throws Exception {
        System.setProperty("increments", " 5 , 10 , 15 ");

        // Create a new instance to reinitialize static fields
        new TestScalabilityAbstractSuite();

        Field incrementsField = ScalabilityAbstractSuite.class.getDeclaredField("INCREMENTS");
        incrementsField.setAccessible(true);
        List<String> increments = (List<String>) incrementsField.get(null);

        // Clear and populate the list with new values
        increments.clear();
        increments.addAll(getIncrementsList(System.getProperty("increments", "1,2,5")));


        assertEquals(3, increments.size());
        assertEquals("5", increments.get(0));
        assertEquals("10", increments.get(1));
        assertEquals("15", increments.get(2));
    }

    @Test
    public void testEmptyValuesFiltering() throws Exception {
        System.setProperty("increments", "5,,10,,15");

        // Create a new instance to reinitialize static fields
        new TestScalabilityAbstractSuite();

        Field incrementsField = ScalabilityAbstractSuite.class.getDeclaredField("INCREMENTS");
        incrementsField.setAccessible(true);
        List<String> increments = (List<String>) incrementsField.get(null);

        // Clear and populate the list with new values
        increments.clear();
        increments.addAll(getIncrementsList(System.getProperty("increments", "1,2,5")));

        assertEquals(3, increments.size());
        assertEquals("5", increments.get(0));
        assertEquals("10", increments.get(1));
        assertEquals("15", increments.get(2));
    }

    // Test subclass to access protected members
    private static class TestScalabilityAbstractSuite extends ScalabilityAbstractSuite {
        @Override
        protected void executeBenchmark(org.apache.jackrabbit.oak.scalability.benchmarks.ScalabilityBenchmark benchmark,
                                        ExecutionContext context) throws Exception {
            // No implementation needed for testing
        }

        @Override
        public ScalabilitySuite addBenchmarks(ScalabilityBenchmark... benchmarks) {
            return null;
        }
    }

}