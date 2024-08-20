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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class CompressionTest {

    @Test
    public void randomized() {
        Random r = new Random();
        for (int i = 0; i < 2000; i++) {
            byte[] data = new byte[r.nextInt(1000)];
            for (int j = 0; j < data.length; j++) {
                // less random first, and then more random
                data[j] = (byte) r.nextInt(1 + (i / 10));
            }
            byte[] comp = Compression.LZ4.compress(data);
            byte[] test = Compression.LZ4.expand(comp);
            assertEquals(data.length, test.length);
            assertTrue(Arrays.equals(data, test));
        }

    }
}
