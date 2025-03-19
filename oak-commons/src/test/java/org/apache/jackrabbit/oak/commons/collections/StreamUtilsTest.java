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
package org.apache.jackrabbit.oak.commons.collections;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamUtilsTest {

    @Test
    public void iteratorToStream() {
        List<String> input = List.of("a", "b", "c");
        Iterator<String> iterator = input.iterator();
        Stream<String> stream = StreamUtils.toStream(iterator);
        List<String> result = stream.collect(Collectors.toList());
        Assert.assertEquals(input.toString(), result.toString());
    }

    @Test(expected = NullPointerException.class)
    public void iteratorToStreamNull() {
        StreamUtils.toStream((Iterator)null);
    }

    @Test
    public void iterableToStream() {
        List<String> input = List.of("a", "b", "c");
        Stream<String> stream = StreamUtils.toStream(input);
        List<String> result = stream.collect(Collectors.toList());
        Assert.assertEquals(input.toString(), result.toString());
    }

    @Test(expected = NullPointerException.class)
    public void iterableStreamNull() {
        StreamUtils.toStream((Iterable)null);
    }
}