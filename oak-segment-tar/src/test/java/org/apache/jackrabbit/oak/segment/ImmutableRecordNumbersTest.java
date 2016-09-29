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

package org.apache.jackrabbit.oak.segment;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;
import org.junit.Test;

public class ImmutableRecordNumbersTest {

    @Test
    public void tableShouldBeCorrectlyInitialized() {
        Map<Integer, Integer> entries = new HashMap<>();

        entries.put(1, 2);
        entries.put(3, 4);
        entries.put(5, 6);

        ImmutableRecordNumbers table = new ImmutableRecordNumbers(entries);

        assertEquals(2, table.getOffset(1));
        assertEquals(4, table.getOffset(3));
        assertEquals(6, table.getOffset(5));
    }

    @Test
    public void changingInitializationMapShouldBeSafe() {
        Map<Integer, Integer> entries = new HashMap<>();

        entries.put(1, 2);
        entries.put(3, 4);
        entries.put(5, 6);

        ImmutableRecordNumbers table = new ImmutableRecordNumbers(entries);

        entries.put(1, 3);
        entries.put(7, 8);
        entries.remove(3);

        assertEquals(2, table.getOffset(1));
        assertEquals(4, table.getOffset(3));
        assertEquals(6, table.getOffset(5));
    }

    @Test
    public void iteratingShouldBeCorrect() {
        Map<Integer, Integer> entries = new HashMap<>();

        entries.put(1, 2);
        entries.put(3, 4);
        entries.put(5, 6);

        ImmutableRecordNumbers table = new ImmutableRecordNumbers(entries);

        Map<Integer, Integer> iterated = new HashMap<>();

        for (Entry entry : table) {
            iterated.put(entry.getRecordNumber(), entry.getOffset());
        }

        assertEquals(entries, iterated);
    }

}
