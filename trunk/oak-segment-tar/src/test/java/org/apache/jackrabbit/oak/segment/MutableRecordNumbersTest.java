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

public class MutableRecordNumbersTest {

    @Test
    public void nonExistingRecordNumberShouldReturnSentinel() {
        assertEquals(-1, new MutableRecordNumbers().getOffset(42));
    }

    @Test
    public void lookupShouldReturnOffset() {
        MutableRecordNumbers table = new MutableRecordNumbers();
        int recordNumber = table.addRecord(RecordType.VALUE, 42);
        assertEquals(42, table.getOffset(recordNumber));
    }

    @Test
    public void sizeShouldBeValid() {
        MutableRecordNumbers table = new MutableRecordNumbers();
        assertEquals(0, table.size());
        table.addRecord(RecordType.VALUE, 42);
        assertEquals(1, table.size());
    }

    @Test
    public void iteratingShouldBeCorrect() {
        MutableRecordNumbers table = new MutableRecordNumbers();

        Map<Integer, Integer> expected = new HashMap<>();

        for (int i = 0; i < 100000; i++) {
            expected.put(table.addRecord(RecordType.VALUE, i), i);
        }

        Map<Integer, Integer> iterated = new HashMap<>();

        for (Entry entry : table) {
            iterated.put(entry.getRecordNumber(), entry.getOffset());
        }

        assertEquals(expected, iterated);
    }

}
