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

import static org.junit.Assert.assertEquals;

public class PipelinedUtilsTest {
    @Test
    public void formatPercentage() {
        assertEquals("0.00",  PipelinedUtils.formatAsPercentage(0, 100));
        assertEquals("1.00",  PipelinedUtils.formatAsPercentage(1, 100));
        assertEquals("0.10",  PipelinedUtils.formatAsPercentage(1, 1000));
        assertEquals("0.01",  PipelinedUtils.formatAsPercentage(1, 10_000));
        assertEquals("N/A",  PipelinedUtils.formatAsPercentage(1, 0));
        assertEquals("100.00",  PipelinedUtils.formatAsPercentage(100, 100));
        assertEquals("120.00",  PipelinedUtils.formatAsPercentage(120, 100));
        assertEquals("314.16",  PipelinedUtils.formatAsPercentage(355, 113));
    }
}