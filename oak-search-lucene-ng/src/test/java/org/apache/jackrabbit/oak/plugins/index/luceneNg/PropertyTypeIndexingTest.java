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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import java.util.stream.Stream;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.luceneNg.JcrValue.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * How a property value of a given JCR type is property-indexed for a given declared index type.
 */
public class PropertyTypeIndexingTest {

    private static final String IDX = "/oak:index/test";
    private static final Object NOT_INDEXED = new Object() {
        @Override
        public String toString() {
            return "not indexed";
        }
    };

    static Stream<Arguments> conversionCases() {
        return Stream.of(
                conversion(Type.UNDEFINED, jcrString("hello"), "hello"),
                conversion(Type.UNDEFINED, jcrString("3.0"), "3.0"),
                conversion(Type.UNDEFINED, jcrLong(3L), "3"),
                conversion(Type.UNDEFINED, jcrDouble(3.14), "3.14"),
                conversion(Type.UNDEFINED, jcrDate("2026-10-14T08:00:00.000Z"), "2026-10-14T08:00:00.000Z"),
                conversion(Type.UNDEFINED, jcrBoolean(true), "true"),
                conversion(Type.UNDEFINED, jcrName("nt:folder"), "nt:folder"),
                conversion(Type.UNDEFINED, jcrPath("/content/a"), "/content/a"),
                conversion(Type.UNDEFINED, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), "7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"),
                conversion(Type.UNDEFINED, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), "7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"),
                conversion(Type.UNDEFINED, jcrUri("https://example.com/a"), "https://example.com/a"),
                conversion(Type.UNDEFINED, jcrDecimal("1.5"), "1.5"),

                conversion(Type.STRING, jcrString("hello"), "hello"),
                conversion(Type.STRING, jcrString("3.0"), "3.0"),
                conversion(Type.STRING, jcrLong(3L), "3"),
                conversion(Type.STRING, jcrDouble(3.14), "3.14"),
                conversion(Type.STRING, jcrDate("2026-10-14T08:00:00.000Z"), "2026-10-14T08:00:00.000Z"),
                conversion(Type.STRING, jcrBoolean(true), "true"),
                conversion(Type.STRING, jcrName("nt:folder"), "nt:folder"),
                conversion(Type.STRING, jcrPath("/content/a"), "/content/a"),
                conversion(Type.STRING, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), "7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"),
                conversion(Type.STRING, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), "7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"),
                conversion(Type.STRING, jcrUri("https://example.com/a"), "https://example.com/a"),
                conversion(Type.STRING, jcrDecimal("1.5"), "1.5"),

                conversion(Type.LONG, jcrString("3"), 3L),
                conversion(Type.LONG, jcrString("hello"), NOT_INDEXED),
                conversion(Type.LONG, jcrLong(3L), 3L),
                conversion(Type.LONG, jcrDouble(3.14), 3L),
                conversion(Type.LONG, jcrDate("2026-10-14T08:00:00.000Z"), NOT_INDEXED),
                conversion(Type.LONG, jcrBoolean(true), NOT_INDEXED),
                conversion(Type.LONG, jcrName("nt:folder"), NOT_INDEXED),
                conversion(Type.LONG, jcrPath("/content/a"), NOT_INDEXED),
                conversion(Type.LONG, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.LONG, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.LONG, jcrUri("https://example.com/a"), NOT_INDEXED),
                conversion(Type.LONG, jcrDecimal("1.5"), NOT_INDEXED),

                conversion(Type.DOUBLE, jcrString("3.0"), 3.0),
                conversion(Type.DOUBLE, jcrString("hello"), NOT_INDEXED),
                conversion(Type.DOUBLE, jcrLong(3L), 3.0),
                conversion(Type.DOUBLE, jcrDouble(3.14), 3.14),
                conversion(Type.DOUBLE, jcrDate("2026-10-14T08:00:00.000Z"), NOT_INDEXED),
                conversion(Type.DOUBLE, jcrBoolean(true), NOT_INDEXED),
                conversion(Type.DOUBLE, jcrName("nt:folder"), NOT_INDEXED),
                conversion(Type.DOUBLE, jcrPath("/content/a"), NOT_INDEXED),
                conversion(Type.DOUBLE, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.DOUBLE, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.DOUBLE, jcrUri("https://example.com/a"), NOT_INDEXED),
                conversion(Type.DOUBLE, jcrDecimal("1.5"), NOT_INDEXED),

                conversion(Type.DATE, jcrString("2026-10-14T08:00:00.000Z"), 1791964800000L),
                conversion(Type.DATE, jcrString("hello"), NOT_INDEXED),
                conversion(Type.DATE, jcrLong(3L), NOT_INDEXED),
                conversion(Type.DATE, jcrDouble(3.14), NOT_INDEXED),
                conversion(Type.DATE, jcrDate("2026-10-14T08:00:00.000Z"), 1791964800000L),
                conversion(Type.DATE, jcrBoolean(true), NOT_INDEXED),
                conversion(Type.DATE, jcrName("nt:folder"), NOT_INDEXED),
                conversion(Type.DATE, jcrPath("/content/a"), NOT_INDEXED),
                conversion(Type.DATE, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.DATE, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.DATE, jcrUri("https://example.com/a"), NOT_INDEXED),
                conversion(Type.DATE, jcrDecimal("1.5"), NOT_INDEXED),

                conversion(Type.BOOLEAN, jcrString("hello"), "hello"),
                conversion(Type.BOOLEAN, jcrLong(3L), "3"),
                conversion(Type.BOOLEAN, jcrDouble(3.14), "3.14"),
                conversion(Type.BOOLEAN, jcrDate("2026-10-14T08:00:00.000Z"), NOT_INDEXED),
                conversion(Type.BOOLEAN, jcrBoolean(true), "true"),
                conversion(Type.BOOLEAN, jcrName("nt:folder"), "nt:folder"),
                conversion(Type.BOOLEAN, jcrPath("/content/a"), NOT_INDEXED),
                conversion(Type.BOOLEAN, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.BOOLEAN, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.BOOLEAN, jcrUri("https://example.com/a"), NOT_INDEXED),
                conversion(Type.BOOLEAN, jcrDecimal("1.5"), NOT_INDEXED),

                conversion(Type.NAME, jcrString("hello"), "hello"),
                conversion(Type.NAME, jcrLong(3L), NOT_INDEXED),
                conversion(Type.NAME, jcrDouble(3.14), NOT_INDEXED),
                conversion(Type.NAME, jcrDate("2026-10-14T08:00:00.000Z"), NOT_INDEXED),
                conversion(Type.NAME, jcrBoolean(true), NOT_INDEXED),
                conversion(Type.NAME, jcrName("nt:folder"), "nt:folder"),
                conversion(Type.NAME, jcrPath("/content/a"), NOT_INDEXED),
                conversion(Type.NAME, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.NAME, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.NAME, jcrUri("https://example.com/a"), NOT_INDEXED),
                conversion(Type.NAME, jcrDecimal("1.5"), NOT_INDEXED),

                conversion(Type.PATH, jcrString("hello"), "hello"),
                conversion(Type.PATH, jcrLong(3L), NOT_INDEXED),
                conversion(Type.PATH, jcrDouble(3.14), NOT_INDEXED),
                conversion(Type.PATH, jcrDate("2026-10-14T08:00:00.000Z"), NOT_INDEXED),
                conversion(Type.PATH, jcrBoolean(true), NOT_INDEXED),
                conversion(Type.PATH, jcrName("nt:folder"), NOT_INDEXED),
                conversion(Type.PATH, jcrPath("/content/a"), "/content/a"),
                conversion(Type.PATH, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.PATH, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.PATH, jcrUri("https://example.com/a"), NOT_INDEXED),
                conversion(Type.PATH, jcrDecimal("1.5"), NOT_INDEXED),

                conversion(Type.REFERENCE, jcrString("hello"), "hello"),
                conversion(Type.REFERENCE, jcrLong(3L), NOT_INDEXED),
                conversion(Type.REFERENCE, jcrDouble(3.14), NOT_INDEXED),
                conversion(Type.REFERENCE, jcrDate("2026-10-14T08:00:00.000Z"), NOT_INDEXED),
                conversion(Type.REFERENCE, jcrBoolean(true), NOT_INDEXED),
                conversion(Type.REFERENCE, jcrName("nt:folder"), NOT_INDEXED),
                conversion(Type.REFERENCE, jcrPath("/content/a"), NOT_INDEXED),
                conversion(Type.REFERENCE, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), "7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"),
                conversion(Type.REFERENCE, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), "7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"),
                conversion(Type.REFERENCE, jcrUri("https://example.com/a"), NOT_INDEXED),
                conversion(Type.REFERENCE, jcrDecimal("1.5"), NOT_INDEXED),

                conversion(Type.WEAKREFERENCE, jcrString("hello"), "hello"),
                conversion(Type.WEAKREFERENCE, jcrLong(3L), NOT_INDEXED),
                conversion(Type.WEAKREFERENCE, jcrDouble(3.14), NOT_INDEXED),
                conversion(Type.WEAKREFERENCE, jcrDate("2026-10-14T08:00:00.000Z"), NOT_INDEXED),
                conversion(Type.WEAKREFERENCE, jcrBoolean(true), NOT_INDEXED),
                conversion(Type.WEAKREFERENCE, jcrName("nt:folder"), NOT_INDEXED),
                conversion(Type.WEAKREFERENCE, jcrPath("/content/a"), NOT_INDEXED),
                conversion(Type.WEAKREFERENCE, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), "7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"),
                conversion(Type.WEAKREFERENCE, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), "7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"),
                conversion(Type.WEAKREFERENCE, jcrUri("https://example.com/a"), NOT_INDEXED),
                conversion(Type.WEAKREFERENCE, jcrDecimal("1.5"), NOT_INDEXED),

                conversion(Type.URI, jcrString("hello"), "hello"),
                conversion(Type.URI, jcrLong(3L), NOT_INDEXED),
                conversion(Type.URI, jcrDouble(3.14), NOT_INDEXED),
                conversion(Type.URI, jcrDate("2026-10-14T08:00:00.000Z"), NOT_INDEXED),
                conversion(Type.URI, jcrBoolean(true), NOT_INDEXED),
                conversion(Type.URI, jcrName("nt:folder"), NOT_INDEXED),
                conversion(Type.URI, jcrPath("/content/a"), NOT_INDEXED),
                conversion(Type.URI, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.URI, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.URI, jcrUri("https://example.com/a"), "https://example.com/a"),
                conversion(Type.URI, jcrDecimal("1.5"), NOT_INDEXED),

                conversion(Type.DECIMAL, jcrString("1.5"), 1.5),
                conversion(Type.DECIMAL, jcrString("hello"), NOT_INDEXED),
                conversion(Type.DECIMAL, jcrLong(3L), 3.0),
                conversion(Type.DECIMAL, jcrDouble(3.14), 3.14),
                conversion(Type.DECIMAL, jcrDate("2026-10-14T08:00:00.000Z"), NOT_INDEXED),
                conversion(Type.DECIMAL, jcrBoolean(true), NOT_INDEXED),
                conversion(Type.DECIMAL, jcrName("nt:folder"), NOT_INDEXED),
                conversion(Type.DECIMAL, jcrPath("/content/a"), NOT_INDEXED),
                conversion(Type.DECIMAL, jcrReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.DECIMAL, jcrWeakReference("7f3c1a2e-5b6d-4c8e-9f01-23456789abcd"), NOT_INDEXED),
                conversion(Type.DECIMAL, jcrUri("https://example.com/a"), NOT_INDEXED),
                conversion(Type.DECIMAL, jcrDecimal("1.5"), 1.5));
    }

    @ParameterizedTest(name = "{0} <- {1}: {2}")
    @MethodSource("conversionCases")
    void indexesValueAsDeclaredType(Type<?> targetType, JcrValue source, Object expected) throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        NodeBuilder defnBuilder = root.child("oak:index").child("test");
        IndexDefinitionBuilder.PropertyRule property = new IndexDefinitionBuilder(defnBuilder).noAsync()
                .indexRule("nt:unstructured").property("value").propertyIndex();
        if (targetType != Type.UNDEFINED) {
            property.type(PropertyType.nameFromValue(targetType.tag()));
        }
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE_NG);
        NodeBuilder node = root.child("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty(source.property("value"));

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            if (expected == NOT_INDEXED) {
                assertNull(FieldInfos.getMergedFieldInfos(reader).fieldInfo("value"));
            } else {
                assertEquals(1, new IndexSearcher(reader).count(exactMatch(expected)));
            }
        }
    }

    private static Query exactMatch(Object expected) {
        if (expected instanceof Long) {
            return LongPoint.newExactQuery("value", (Long) expected);
        }
        if (expected instanceof Double) {
            return DoublePoint.newExactQuery("value", (Double) expected);
        }
        return new TermQuery(new Term("value", (String) expected));
    }

    private static Arguments conversion(Type<?> targetType, JcrValue source, Object expected) {
        return Arguments.of(targetType, source, expected);
    }
}
