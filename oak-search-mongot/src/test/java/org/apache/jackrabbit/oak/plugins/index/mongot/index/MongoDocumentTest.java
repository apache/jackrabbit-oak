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
package org.apache.jackrabbit.oak.plugins.index.mongot.index;

import java.util.List;

import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class MongoDocumentTest {

    @Test
    public void keepsTypedAndAnalyzedValuesInSeparateFields() {
        MongoDocument document = new MongoDocument("/content/a");
        document.addTypedProperty("price", 42L);
        document.addAnalyzedProperty("jcr:title", "MongoDB Oak");
        document.addFulltext("MongoDB Oak");

        assertEquals("/content/a", document.toBson().getString(MongoFieldNames.PATH));
        assertEquals(42L, document.toBson()
                .get(MongoFieldNames.TYPED, Document.class).get("cHJpY2U"));
        assertEquals(List.of("MongoDB Oak"), document.toBson()
                .get(MongoFieldNames.ANALYZED, Document.class)
                .getList("amNyOnRpdGxl", String.class));
        assertEquals(List.of("MongoDB Oak"),
                document.toBson().getList(MongoFieldNames.FULLTEXT, String.class));
    }

    @Test
    public void recordsRepositoryStructureForPathRestrictions() {
        MongoDocument document = new MongoDocument("/content/site/a");

        assertEquals("/content/site", document.toBson().getString(MongoFieldNames.PARENT));
        assertEquals(3, document.toBson().getInteger(MongoFieldNames.DEPTH).intValue());
        assertEquals(List.of("/", "/content", "/content/site"),
                document.toBson().getList(MongoFieldNames.ANCESTORS, String.class));
    }

    @Test
    public void preservesMultipleTypedValuesAndDeduplicatesSearchText() {
        MongoDocument document = new MongoDocument("/content/a");
        document.addTypedProperty("tags", "one");
        document.addTypedProperty("tags", "two");
        document.addFulltext("same");
        document.addFulltext("same");

        assertEquals(List.of("one", "two"), document.toBson()
                .get(MongoFieldNames.TYPED, Document.class)
                .getList("dGFncw", String.class));
        assertEquals(List.of("same"),
                document.toBson().getList(MongoFieldNames.FULLTEXT, String.class));
    }

    @Test
    public void encodesNamespaceAndRelativePropertyNamesWithoutMongoSyntax() {
        String encoded = MongoFieldNames.encodeProperty("jcr:content/meta.title");

        assertEquals("amNyOmNvbnRlbnQvbWV0YS50aXRsZQ", encoded);
        assertFalse(encoded.contains("."));
        assertFalse(encoded.contains("$"));
    }

    @Test
    public void recordsDynamicBoostTokensWithoutFlatteningConfidence() {
        MongoDocument document = new MongoDocument("/content/a");

        document.addDynamicBoost("tags", "mongodb", 0.8d);

        Document boost = document.toBson().get(MongoFieldNames.DYNAMIC_BOOST, Document.class);
        Document entry = boost.getList(MongoFieldNames.encodeProperty("tags"), Document.class).get(0);
        assertEquals("mongodb", entry.getString("token"));
        assertEquals(0.8d, entry.getDouble("confidence"), 0.0d);
    }

    @Test
    public void hashesOnlyDocumentIdsThatExceedTheSearchLimit() {
        String path = "/content/" + "x".repeat(33_000);

        Document first = new MongoDocument(path).toBson();
        Document second = new MongoDocument(path).toBson();

        assertNotEquals(path, first.getString(MongoFieldNames.ID));
        assertEquals(64, first.getString(MongoFieldNames.ID).length());
        assertEquals(first.getString(MongoFieldNames.ID), second.getString(MongoFieldNames.ID));
        assertEquals(path, first.getString(MongoFieldNames.PATH));
    }
}
