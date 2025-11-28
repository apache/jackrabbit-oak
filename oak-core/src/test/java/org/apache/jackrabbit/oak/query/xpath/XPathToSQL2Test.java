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
package org.apache.jackrabbit.oak.query.xpath;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;

import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for XPathToSQL2Converter focusing on the OR-to-UNION conversion behavior
 * controlled by the optimizeXPathUnion feature toggle.
 */
public class XPathToSQL2Test {

    @Test
    public void testOrToUnionConversionFeatureDisabled() throws ParseException {
        // Default behavior (feature disabled): OR conditions should be converted to UNION
        verify("//*[@a = 1 or @b = 2]",
                false,
                    "select ... where [a] = 1 union select ... where [b] = 2 ");
    }

    @Test
    public void testOrToUnionConversionFeatureEnabled() throws ParseException {
        // When feature is enabled: OR conditions should NOT be converted to UNION
        verify("//*[@a = 1 or @b = 2]",
                true,
                "select ... where [a] = 1 or [b] = 2 ");
    }

    @Test
    public void testMultipleOrConditionsFeatureDisabled() throws ParseException {
        // Test multiple OR conditions with feature disabled (default)
        verify("//*[@a = 1 or @b = 2 or @c = 3]",
                false, 
                "select ... where [a] = 1 union select ... where [b] = 2 union select ... where [c] = 3 ");
    }
 
    @Test
    public void testMultipleOrConditionsFeatureEnabled() throws ParseException {
        // Test multiple OR conditions with feature enabled
        verify("//*[@a = 1 or @b = 2 or @c = 3]", 
                true, 
                "select ... where [a] = 1 or [b] = 2 or [c] = 3 ");
    }
 
    @Test
    public void testNestedOrWithAndFeatureDisabled() throws ParseException {
        // Test nested OR with AND conditions, feature disabled
        verify("//*[@x = 1 and (@a = 1 or @b = 2)]", 
                false, 
                "select ... where [x] = 1 and [a] = 1 union select ... where [x] = 1 and [b] = 2 ");
    }
 
    @Test
    public void testNestedOrWithAndFeatureEnabled() throws ParseException {
        // Test nested OR with AND conditions, feature enabled
        verify("//*[@x = 1 and (@a = 1 or @b = 2)]", 
                true, 
                "select ... where [x] = 1 and ([a] = 1 or [b] = 2) ");
    }
 
    @Test
    public void testSimpleOrQueryFeatureDisabled() throws ParseException {
        // Simple OR query with feature disabled
        verify("//*[@name = 'foo' or @name = 'bar']", 
                false, 
                "select ... where [name] in('foo', 'bar') ");
    }
 
    @Test
    public void testSimpleOrQueryFeatureEnabled() throws ParseException {
        // Simple OR query with feature enabled
        verify("//*[@name = 'foo' or @name = 'bar']", 
                true, 
                "select ... where [name] in('foo', 'bar') ");
    }
 
    @Test
    public void testComplexNestedOrFeatureDisabled() throws ParseException {
        // Complex nested query: (a AND (b OR c)) OR d
        verify("//*[(@a = 1 and (@b = 2 or @c = 3)) or @d = 4]", 
                false, 
                "select ... where [a] = 1 and [b] = 2 union select ... where [a] = 1 and [c] = 3 union select ... where [d] = 4 ");
    }
 
    @Test
    public void testComplexNestedOrFeatureEnabled() throws ParseException {
        // Complex nested query: (a AND (b OR c)) OR d
        verify("//*[(@a = 1 and (@b = 2 or @c = 3)) or @d = 4]", 
                true, 
                "select ... where [a] = 1 and ([b] = 2 or [c] = 3) or [d] = 4 ");
    }
 
    @Test
    public void testOrWithDifferentPathsFeatureDisabled() throws ParseException {
        // Test OR conditions with different paths
        verify("//*[@title = 'Test' or @desc = 'Test']", 
                false, 
                "select ... where [title] = 'Test' union select ... where [desc] = 'Test' ");
    }
 
    @Test
    public void testOrWithDifferentPathsFeatureEnabled() throws ParseException {
        // Test OR conditions with different paths
        verify("//*[@title = 'Test' or @desc = 'Test']", 
                true, 
                "select ... where [title] = 'Test' or [desc] = 'Test' ");
    }
 
    @Test
    public void testOrWithFunctionCallFeatureDisabled() throws ParseException {
        // Test OR with function calls
        verify("//*[jcr:contains(., 'test') or @type = 'page']", 
                false, 
                "select ... where contains(*, 'test') union select ... where [type] = 'page' ");
    }
 
    @Test
    public void testOrWithFunctionCallFeatureEnabled() throws ParseException {
        verify("//*[jcr:contains(., 'test') or @type = 'page']", 
                true, 
                "select ... where contains(*, 'test') or [type] = 'page' ");
    }
 
    /**
     * Helper method to create a Feature mock with the specified enabled state.
     */
    private static Feature createFeature(boolean enabled) {
        Feature feature = Mockito.mock(Feature.class);
        Mockito.when(feature.isEnabled()).thenReturn(enabled);
        return feature;
    }

    private void verify(String xpath, boolean optimizeXPathUnion, String expectedSql2) throws ParseException {
        Feature feature = createFeature(optimizeXPathUnion);
        QueryEngineSettings settings = new QueryEngineSettings();
        settings.setOptimizeXPathUnion(feature);
        XPathToSQL2Converter converter = new XPathToSQL2Converter(settings);
        String sql2 = converter.convert(xpath);
        sql2 = formatSQL(sql2);
        assertEquals(expectedSql2, sql2);
    }

    static String formatSQL(String sql) { 
        sql = sql.replace('\n', ' ');
        sql = sql.replaceAll("\\[jcr:path\\], \\[jcr:score\\], \\* from \\[nt:base\\] as a", "...");
        sql = sql.replaceAll("\\/\\*.*\\*/", "");
        return sql;
    }
}

