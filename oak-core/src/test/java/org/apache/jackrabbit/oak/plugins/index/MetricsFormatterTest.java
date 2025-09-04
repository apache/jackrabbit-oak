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
package org.apache.jackrabbit.oak.plugins.index;

import org.junit.Test;

import static org.junit.Assert.*;

public class MetricsFormatterTest {

    @Test
    public void testAdd() {
        MetricsFormatter metricsFormatter = MetricsFormatter.newBuilder();
        String result = metricsFormatter.add("key", "value")
                .add("key", 1)
                .add("key", 1L)
                .add("key", true)
                .build();
        assertEquals("{\"key\":\"value\",\"key\":1,\"key\":1,\"key\":true}", result);
        assertThrows(IllegalStateException.class, () -> metricsFormatter.add("key", "value"));
        assertEquals("{\"key\":\"value\",\"key\":1,\"key\":1,\"key\":true}", metricsFormatter.build());
    }
}