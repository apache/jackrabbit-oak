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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;

/**
 * Tests for variable size integer streaming.
 */
public class VarIntTest {

    @Test
    public void test() throws IOException {
        for (int i = 1; i > 0; i *= 2) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            NodeStreamConverter.writeVarInt(out, i);
            ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
            int test = NodeStreamReader.readVarInt(in);
            assertEquals(test, i);
            assertEquals(-1, in.read());
        }
    }
}
