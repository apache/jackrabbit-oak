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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedUtils.formatAsPercentage;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedUtils.formatAsTransferSpeedMBs;
import static org.junit.Assert.assertEquals;

public class PipelinedUtilsTest {
    @Test
    public void testFormatAsPercentage() {
        assertEquals("0.00", formatAsPercentage(0, 100));
        assertEquals("1.00", formatAsPercentage(1, 100));
        assertEquals("0.10", formatAsPercentage(1, 1000));
        assertEquals("0.01", formatAsPercentage(1, 10_000));
        assertEquals("N/A", formatAsPercentage(1, 0));
        assertEquals("100.00", formatAsPercentage(100, 100));
        assertEquals("120.00", formatAsPercentage(120, 100));
        assertEquals("314.16", formatAsPercentage(355, 113));
    }

    @Test
    public void testFormatAsTransferSpeedMBs() {
        System.out.println("max: " + Long.MAX_VALUE);
        assertEquals("0.95 MB/s", formatAsTransferSpeedMBs(1_000_000, TimeUnit.SECONDS.toMillis(1)));
        assertEquals("0.00 MB/s", formatAsTransferSpeedMBs(0, TimeUnit.SECONDS.toMillis(1)));
        assertEquals("0.00 MB/s", formatAsTransferSpeedMBs(1, TimeUnit.SECONDS.toMillis(1)));
        assertEquals("N/A", formatAsTransferSpeedMBs(1_000_000, TimeUnit.SECONDS.toMillis(0)));
        assertEquals("8796093022208.00 MB/s", formatAsTransferSpeedMBs(Long.MAX_VALUE, TimeUnit.SECONDS.toMillis(1)));
        assertEquals("-8796093022208.00 MB/s", formatAsTransferSpeedMBs(Long.MIN_VALUE, TimeUnit.SECONDS.toMillis(1)));
        assertEquals("0.00 MB/s", formatAsTransferSpeedMBs(1_000_000, Long.MAX_VALUE));
        assertEquals("-0.00 MB/s", formatAsTransferSpeedMBs(1_000_000, Long.MIN_VALUE));
    }
}