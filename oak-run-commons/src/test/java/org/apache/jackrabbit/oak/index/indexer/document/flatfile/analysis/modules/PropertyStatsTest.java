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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Random;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;
import org.junit.Test;

public class PropertyStatsTest {

    @Test
    public void manyUniqueProperties() {
        PropertyStats pc = new PropertyStats(false, 42);
        pc.setSkip(0);
        for (int i = 0; i < 1_000_000; i++) {
            NodeProperty p = new NodeProperty("unique" + i, ValueType.STRING, "");
            NodeData n = new NodeData(Arrays.asList(""), Arrays.asList(p));
            pc.add(n);
        }
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 1000; j++) {
                NodeProperty p = new NodeProperty("common" + i, ValueType.STRING, "x" + (j % 5));
                NodeData n = new NodeData(Arrays.asList(""), Arrays.asList(p));
                pc.add(n);
            }
        }
        assertEquals("PropertyStats\n"
                + "common0 weight 5 count 1000 distinct 5 avgSize 1 maxSize 2\n"
                + "common1 weight 5 count 1000 distinct 5 avgSize 1 maxSize 2\n"
                + "common2 weight 5 count 1000 distinct 5 avgSize 1 maxSize 2\n"
                + "", pc.toString());
    }

    @Test
    public void skewed() {
        PropertyStats pc = new PropertyStats(false, 42);
        pc.setSkip(0);
        Random r = new Random(1);
        for (int i = 0; i < 1_000_000; i++) {
            // in 50% of the cases, the value is either true or false
            // and in the remaining cases, it is unique
            String value = r.nextInt(100) < 50 ? "" + r.nextBoolean() : "" + r.nextInt();
            NodeProperty p = new NodeProperty("skewed", ValueType.STRING, value);
            NodeData n = new NodeData(Arrays.asList(""), Arrays.asList(p));
            pc.add(n);
        }
        assertEquals("PropertyStats\n"
                + "skewed weight 3 count 1000000 distinct 394382 avgSize 7 maxSize 11 top {\"skipped\":899091,\"counted\":90910,\"false\":25583,\"true\":25518,\"-411461567\":1,\"1483286044\":1,\"1310925467\":1,\"-1752252714\":1,\"-1433290908\":1,\"-1209544007\":1}\n"
                + "", pc.toString());
    }

}
