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

package org.apache.jackrabbit.oak.segment.file.proc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Record;
import org.junit.Test;

public class RecordsNodeTest {

    @Test
    public void shouldExposeRecordNumber() {
        Record record = mock(Record.class);
        when(record.getNumber()).thenReturn(1);

        Backend backend = mock(Backend.class);
        when(backend.getSegmentRecords("s")).thenReturn(Optional.of(Collections.singletonList(record)));

        assertTrue(new RecordsNode(backend, "s").hasChildNode("1"));
    }

    @Test
    public void shouldExposeAllRecordNumbers() {
        Set<Integer> numbers = Sets.newHashSet(1, 2, 3);

        Set<Record> records = numbers.stream()
            .map(RecordsNodeTest::newRecord)
            .collect(Collectors.toSet());

        Backend backend = mock(Backend.class);
        when(backend.getSegmentRecords("s")).thenReturn(Optional.of(records));

        Set<String> names = numbers.stream()
            .map(Object::toString)
            .collect(Collectors.toSet());

        assertEquals(names, Sets.newHashSet(new RecordsNode(backend, "s").getChildNodeNames()));
    }

    private static Record newRecord(Integer number) {
        Record record = mock(Record.class);
        when(record.getNumber()).thenReturn(number);
        return record;
    }

}
