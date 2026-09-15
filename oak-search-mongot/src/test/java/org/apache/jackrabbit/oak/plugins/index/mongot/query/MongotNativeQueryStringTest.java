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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongotNativeQueryStringTest {

    @Test
    public void rewritesFieldScopedClausesToAnalyzedProperties() {
        assertEquals(queryString("analyzed.dGl0bGU:foo -analyzed.dGl0bGU:bar"),
                MongotNativeQueryString.operator("title:foo -title:bar"));
    }

    @Test
    public void preservesUnfieldedBooleanSyntax() {
        assertEquals(queryString("foo AND (bar OR baz)"),
                MongotNativeQueryString.operator("foo AND (bar OR baz)"));
    }

    @Test
    public void preservesUnfieldedRegularExpression() {
        assertEquals(queryString("/b.*/"), MongotNativeQueryString.operator("/b.*/"));
    }

    @Test
    public void decodesEscapedPunctuationInFieldName() {
        assertEquals(queryString("analyzed.amNyOnRpdGxl:foo"),
                MongotNativeQueryString.operator("jcr\\:title:foo"));
    }

    private static Document queryString(String query) {
        return new Document("queryString", new Document("defaultPath", "_fulltext")
                .append("query", query));
    }
}
