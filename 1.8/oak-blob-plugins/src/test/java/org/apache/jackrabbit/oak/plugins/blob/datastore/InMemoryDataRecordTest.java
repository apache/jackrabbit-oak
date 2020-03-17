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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.ByteArrayInputStream;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InMemoryDataRecordTest {
    @Test
    public void testGetInstance() throws Exception {
        int length = 400;
        byte[] data = new byte[length];
        new Random().nextBytes(data);

        DataRecord dr = InMemoryDataRecord.getInstance(data);
        assertTrue(InMemoryDataRecord.isInstance(dr.getIdentifier().toString()));

        DataRecord dr2 = InMemoryDataRecord.getInstance(dr.getIdentifier().toString());

        assertTrue(IOUtils.contentEquals(dr.getStream(), dr2.getStream()));
        assertTrue(IOUtils.contentEquals(dr.getStream(), new ByteArrayInputStream(data)));

        assertEquals(length, dr.getLength());
        assertEquals(dr2.getLength(), dr.getLength());

        assertEquals(dr, dr2);
    }
}
