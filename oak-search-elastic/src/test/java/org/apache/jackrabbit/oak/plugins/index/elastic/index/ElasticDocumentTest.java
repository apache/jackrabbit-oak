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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import org.junit.After;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticDocumentTest {

    @After
    public void resetToggle() {
        ElasticDocument.FT_OAK_12353_ENABLE.set(false);
    }

    @Test
    public void dynamicBoostValuesAreNotGroupedByDefault() {
        ElasticDocument doc = new ElasticDocument("/test");
        doc.addDynamicBoostField("predictedTagsDynamicBoost", "Replacement Cost", 1.0);
        doc.addDynamicBoostField("predictedTagsDynamicBoost", "Theft", 1.0);
        doc.addDynamicBoostField("predictedTagsDynamicBoost", "GENERAL INSURANCE COMPANY", 0.988);

        Object value = doc.getProperties().get("predictedTagsDynamicBoost");
        assertTrue(value instanceof Set);
        @SuppressWarnings("unchecked")
        Set<Map<String, Object>> nestedDocs = (Set<Map<String, Object>>) value;
        assertEquals(3, nestedDocs.size());
        for (Map<String, Object> nestedDoc : nestedDocs) {
            assertTrue(nestedDoc.get(ElasticIndexHelper.DYNAMIC_BOOST_NESTED_VALUE) instanceof String);
        }
    }

    @Test
    public void dynamicBoostValuesAreGroupedByBoostWhenToggleEnabled() {
        ElasticDocument.FT_OAK_12353_ENABLE.set(true);

        ElasticDocument doc = new ElasticDocument("/test");
        doc.addDynamicBoostField("predictedTagsDynamicBoost", "Replacement Cost", 1.0);
        doc.addDynamicBoostField("predictedTagsDynamicBoost", "Theft", 1.0);
        doc.addDynamicBoostField("predictedTagsDynamicBoost", "Alberta", 1.0);
        doc.addDynamicBoostField("predictedTagsDynamicBoost", "GENERAL INSURANCE COMPANY", 0.988);

        Object value = doc.getProperties().get("predictedTagsDynamicBoost");
        assertTrue(value instanceof Set);
        @SuppressWarnings("unchecked")
        Set<Map<String, Object>> nestedDocs = (Set<Map<String, Object>>) value;
        // one nested doc for the 3 values sharing boost=1.0, one for the distinct boost=0.988
        assertEquals(2, nestedDocs.size());

        boolean foundGrouped = false;
        boolean foundSingle = false;
        for (Map<String, Object> nestedDoc : nestedDocs) {
            Object boost = nestedDoc.get(ElasticIndexHelper.DYNAMIC_BOOST_NESTED_BOOST);
            Object nestedValue = nestedDoc.get(ElasticIndexHelper.DYNAMIC_BOOST_NESTED_VALUE);
            if (Double.valueOf(1.0).equals(boost)) {
                assertTrue(nestedValue instanceof List);
                @SuppressWarnings("unchecked")
                List<String> values = (List<String>) nestedValue;
                assertEquals(List.of("Replacement Cost", "Theft", "Alberta"), values);
                foundGrouped = true;
            } else if (Double.valueOf(0.988).equals(boost)) {
                assertEquals("GENERAL INSURANCE COMPANY", nestedValue);
                foundSingle = true;
            }
        }
        assertTrue(foundGrouped);
        assertTrue(foundSingle);
    }

    @Test
    public void singleDynamicBoostGroupIsNotWrappedInCollection() {
        ElasticDocument.FT_OAK_12353_ENABLE.set(true);

        ElasticDocument doc = new ElasticDocument("/test");
        doc.addDynamicBoostField("predictedTagsDynamicBoost", "Replacement Cost", 1.0);
        doc.addDynamicBoostField("predictedTagsDynamicBoost", "Theft", 1.0);

        Object value = doc.getProperties().get("predictedTagsDynamicBoost");
        assertTrue(value instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> nestedDoc = (Map<String, Object>) value;
        assertEquals(List.of("Replacement Cost", "Theft"), nestedDoc.get(ElasticIndexHelper.DYNAMIC_BOOST_NESTED_VALUE));
        assertEquals(1.0, nestedDoc.get(ElasticIndexHelper.DYNAMIC_BOOST_NESTED_BOOST));
    }
}
