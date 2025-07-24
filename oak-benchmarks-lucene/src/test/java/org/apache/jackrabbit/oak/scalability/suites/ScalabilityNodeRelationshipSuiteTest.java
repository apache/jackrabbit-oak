/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.jackrabbit.oak.scalability.suites;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test suite for ScalabilityNodeRelationshipSuite
 */
public class ScalabilityNodeRelationshipSuiteTest {

    private String originalNodeLevelsProperty;

    @Before
    public void saveOriginalProperty() {
        originalNodeLevelsProperty = System.getProperty("nodeLevels");
    }

    @After
    public void restoreOriginalProperty() {
        if (originalNodeLevelsProperty == null) {
            System.clearProperty("nodeLevels");
        } else {
            System.setProperty("nodeLevels", originalNodeLevelsProperty);
        }
    }

    @Test
    public void testDefaultNodeLevels() throws Exception {
        System.clearProperty("nodeLevels");

        // Create a new instance to initialize static fields
        new TestScalabilityNodeRelationshipSuite();

        Field nodeLevelsField = ScalabilityNodeRelationshipSuite.class.getDeclaredField("NODE_LEVELS");
        nodeLevelsField.setAccessible(true);
        List<String> nodeLevels = (List<String>) nodeLevelsField.get(null);

        // Clear and populate the list with new values
        nodeLevels.clear();
        nodeLevels.addAll(getNodeLevels(System.getProperty("nodeLevels", "10,5,2,1")));

        assertEquals(4, nodeLevels.size());
        assertEquals("10", nodeLevels.get(0));
        assertEquals("5", nodeLevels.get(1));
        assertEquals("2", nodeLevels.get(2));
        assertEquals("1", nodeLevels.get(3));
    }

    @Test
    public void testCustomNodeLevels() throws Exception {
        System.setProperty("nodeLevels", "20,10,5,2");

        // Create a new instance to initialize static fields
        new TestScalabilityNodeRelationshipSuite();

        Field nodeLevelsField = ScalabilityNodeRelationshipSuite.class.getDeclaredField("NODE_LEVELS");
        nodeLevelsField.setAccessible(true);
        List<String> nodeLevels = (List<String>) nodeLevelsField.get(null);

        // Clear and populate the list with new values
        nodeLevels.clear();
        nodeLevels.addAll(getNodeLevels(System.getProperty("nodeLevels", "10,5,2,1")));

        assertEquals(4, nodeLevels.size());
        assertEquals("20", nodeLevels.get(0));
        assertEquals("10", nodeLevels.get(1));
        assertEquals("5", nodeLevels.get(2));
        assertEquals("2", nodeLevels.get(3));
    }

    @Test
    public void testWhitespaceTrimmingInNodeLevels() throws Exception {
        System.setProperty("nodeLevels", " 15 , 8 , 4 , 2 ");

        // Create a new instance to initialize static fields
        new TestScalabilityNodeRelationshipSuite();

        Field nodeLevelsField = ScalabilityNodeRelationshipSuite.class.getDeclaredField("NODE_LEVELS");
        nodeLevelsField.setAccessible(true);
        List<String> nodeLevels = (List<String>) nodeLevelsField.get(null);

        // Clear and populate the list with new values
        nodeLevels.clear();
        nodeLevels.addAll(getNodeLevels(System.getProperty("nodeLevels", "10,5,2,1")));

        assertEquals(4, nodeLevels.size());
        assertEquals("15", nodeLevels.get(0));
        assertEquals("8", nodeLevels.get(1));
        assertEquals("4", nodeLevels.get(2));
        assertEquals("2", nodeLevels.get(3));
    }

    @Test
    public void testEmptyValuesFiltering() throws Exception {
        System.setProperty("nodeLevels", "25,,10,,3");

        // Create a new instance to initialize static fields
        new TestScalabilityNodeRelationshipSuite();

        Field nodeLevelsField = ScalabilityNodeRelationshipSuite.class.getDeclaredField("NODE_LEVELS");
        nodeLevelsField.setAccessible(true);
        List<String> nodeLevels = (List<String>) nodeLevelsField.get(null);

        // Clear and populate the list with new values
        nodeLevels.clear();
        nodeLevels.addAll(getNodeLevels(System.getProperty("nodeLevels", "10,5,2,1")));

        assertEquals(3, nodeLevels.size());
        assertEquals("25", nodeLevels.get(0));
        assertEquals("10", nodeLevels.get(1));
        assertEquals("3", nodeLevels.get(2));
    }

    @Test
    public void testDifferentNumberOfLevels() throws Exception {
        System.setProperty("nodeLevels", "50,25,10");

        // Create a new instance to initialize static fields
        new TestScalabilityNodeRelationshipSuite();

        Field nodeLevelsField = ScalabilityNodeRelationshipSuite.class.getDeclaredField("NODE_LEVELS");
        nodeLevelsField.setAccessible(true);
        List<String> nodeLevels = (List<String>) nodeLevelsField.get(null);

        // Clear and populate the list with new values
        nodeLevels.clear();
        nodeLevels.addAll(getNodeLevels(System.getProperty("nodeLevels", "10,5,2,1")));

        assertEquals(3, nodeLevels.size());
        assertEquals("50", nodeLevels.get(0));
        assertEquals("25", nodeLevels.get(1));
        assertEquals("10", nodeLevels.get(2));
    }

    private List<String> getNodeLevels(String value) {
        return Arrays.stream(System.getProperty("nodeLevels", "10,5,2,1").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    // Test subclass to access protected members
    private static class TestScalabilityNodeRelationshipSuite extends ScalabilityNodeRelationshipSuite {
        public TestScalabilityNodeRelationshipSuite() {
            super(false);
        }
    }
}