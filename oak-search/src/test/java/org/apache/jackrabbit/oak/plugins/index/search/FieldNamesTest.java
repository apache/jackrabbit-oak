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
package org.apache.jackrabbit.oak.plugins.index.search;

import org.junit.Assert;
import org.junit.Test;

public class FieldNamesTest {
    @Test
    public void checkPropertyFieldNames() {
        assertPropertyFieldName("abc", true);
        assertPropertyFieldName("a:b", true);
        assertPropertyFieldName("a/b", true);
        assertPropertyFieldName("a/b:c", true);
        assertPropertyFieldName("a:b/c", true);

        assertPropertyFieldName(FieldNames.FULLTEXT_RELATIVE_NODE + "a", false);
        assertPropertyFieldName(FieldNames.ANALYZED_FIELD_PREFIX + "a", false);
        assertPropertyFieldName(FieldNames.FULLTEXT, false);
        assertPropertyFieldName(":abc", false);
        assertPropertyFieldName("abc_facet", false);
        assertPropertyFieldName("a:b_facet", false);
        assertPropertyFieldName("a/b_facet", false);
        assertPropertyFieldName("a/b:c_facet", false);
        assertPropertyFieldName("a:b/c_facet", false);
    }

    private void assertPropertyFieldName(String name, boolean expected) {
        Assert.assertEquals("Check for field name " + name + " doesn't meet expectation - " + expected, FieldNames.isPropertyField(name), expected);
    }
}
