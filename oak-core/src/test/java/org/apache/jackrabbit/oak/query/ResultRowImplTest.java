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
package org.apache.jackrabbit.oak.query;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyValues.newString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultRowImplTest {
    @Test
    public void mappedGetValue() {
        Query query = mock(Query.class);
        when(query.getColumnIndex("origCol")).thenReturn(0);
        when(query.getColumnIndex("col1")).thenReturn(1);

        PropertyValue[] origVals = new PropertyValue[]{newString("origVal"), newString("overriddenVal")};
        ResultRowImpl orig = new ResultRowImpl(query, null, origVals, null, null);

        Map<String, String> map = new HashMap<>();
        map.put("col1", "val1");

        ResultRowImpl mappedRow = ResultRowImpl.getMappingResultRow(orig, map);

        assertEquals("origVal", mappedRow.getValue("origCol").getValue(Type.STRING));
        assertEquals("val1", mappedRow.getValue("col1").getValue(Type.STRING));

        PropertyValue[] mappedVals = mappedRow.getValues();
        assertEquals(2, mappedVals.length);
        assertEquals("origVal", mappedVals[0].getValue(Type.STRING));
        assertEquals("val1", mappedVals[1].getValue(Type.STRING));
    }
}
