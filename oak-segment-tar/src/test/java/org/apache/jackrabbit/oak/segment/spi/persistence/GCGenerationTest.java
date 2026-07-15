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
 *
 */

package org.apache.jackrabbit.oak.segment.spi.persistence;

import static org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

import org.awaitility.Awaitility;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GCGenerationTest {

    @Test
    public void testCompareWith() {
        GCGeneration m = newGCGeneration(0, 0, false);
        GCGeneration n = newGCGeneration(2, 3, false);
        assertEquals(2, n.compareWith(m));
    }

    @Test
    public void testCompareFullGenerationWith() {
        GCGeneration m = newGCGeneration(0, 0, false);
        GCGeneration n = newGCGeneration(2, 3, false);
        assertEquals(3, n.compareFullGenerationWith(m));
    }

    @Test
    public void testObjectReuse() {
        IntFunction<GCGeneration> gcGenerationProducer =
                i -> newGCGeneration(i / 10, i / 2, i % 2 == 0);

        Map<Integer, GCGeneration> generations = IntStream.range(0, 50)
                .boxed()
                .collect(Collectors.toMap(Function.identity(), gcGenerationProducer::apply));

        Map<Integer, Integer> removed = IntStream.of(5, 22, 37)
                .boxed()
                .collect(Collectors.toMap(Function.identity(), i -> System.identityHashCode(generations.remove(i))));

        for (int i = 0; i < 50; i++) {
            if (removed.containsKey(i)) {
                final int index = i;
                Awaitility.await()
                        .atMost(1, TimeUnit.SECONDS)
                        .untilAsserted(() -> {
                            System.gc();
                            assertNotEquals(
                                    removed.get(index).intValue(),
                                    System.identityHashCode(gcGenerationProducer.apply(index)));
                        });

            } else {
                assertSame(
                        generations.get(i),
                        gcGenerationProducer.apply(i));
                assertEquals(
                        System.identityHashCode(generations.get(i)),
                        System.identityHashCode(gcGenerationProducer.apply(i)));
            }
        }
    }
}
