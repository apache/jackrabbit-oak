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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

public class TopKValuesTest {

    @Test
    public void test() {
        TopKValues v = new TopKValues(3);
        Random r = new Random(1);
        for(int i=0; i<1000000; i++) {
            if(r.nextBoolean()) {
                v.add("common" + r.nextInt(2));
            } else {
                v.add("rare" + r.nextInt(100));
            }
        }
        assertEquals("{\"notSkewed\":5,\"skipped\":908191,\"counted\":91809,\"common1\":24849,\"common0\":24652,\"rare13\":2374}", v.toString());
        assertEquals(91809, v.getCount());
        assertEquals(24849, v.getTopCount());
        assertEquals(24652, v.getSecondCount());
    }

}
