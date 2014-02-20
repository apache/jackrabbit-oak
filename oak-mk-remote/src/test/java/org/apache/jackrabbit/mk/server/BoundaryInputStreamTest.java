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
package org.apache.jackrabbit.mk.server;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.Arrays;

import org.apache.jackrabbit.oak.commons.IOUtils;

public class BoundaryInputStreamTest extends TestCase {

    private static final String BOUNDARY = "------ClientFormBoundaryHB5WJrSAPZjfwtqt--";

    private static final Random RANDOM = new Random();

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Create a very small boundary input stream, where the boundary will be
     * split between two reads.
     */
    public void testSmallReads() throws Exception {
        byte[] content = new byte[1024];
        RANDOM.nextBytes(content);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(content);
        out.write("\r\n".getBytes());
        out.write(BOUNDARY.getBytes());

        byte[] noise = new byte[content.length];
        RANDOM.nextBytes(noise);
        out.write(noise);

        BoundaryInputStream in = new BoundaryInputStream(
                new ByteArrayInputStream(out.toByteArray()), BOUNDARY, 1);
        out = new ByteArrayOutputStream(8192);
        IOUtils.copy(in, out);

        byte[] result = out.toByteArray();
        assertEquals(content.length, result.length);
        assertTrue(Arrays.equals(content, result));
    }
}
