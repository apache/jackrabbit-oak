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

package org.apache.jackrabbit.oak.plugins.blob.serializer;

import java.io.File;
import java.io.InputStream;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.serializer.FSBlobSerializer;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FSBlobSerializerTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void blobs() throws Exception{
        int maxInlineSize = 100;
        FSBlobSerializer serializer = new FSBlobSerializer(folder.getRoot(), maxInlineSize);
        String data = Strings.repeat("x", maxInlineSize * 10);

        Blob b = new ArrayBasedBlob(data.getBytes(UTF_8));

        String id = serializer.serialize(b);
        Blob b2 = serializer.deserialize(id);

        assertTrue(AbstractBlob.equal(b, b2));
    }


    @Test
    public void errorBlob() throws Exception{
        FSBlobSerializer serializer = new FSBlobSerializer(folder.getRoot(), 1);
        String blobValue = serializer.serialize(new BadBlob());

        try{
            Blob b = serializer.deserialize(blobValue);
            assertEquals("foo", b.getContentIdentity());
            b.getNewStream();
            fail();
        } catch (RuntimeException ignore){

        }
    }

    private static class BadBlob extends AbstractBlob {

        @Override
        public String getContentIdentity() {
            return "foo";
        }

        @Nonnull
        @Override
        public InputStream getNewStream() {
           throw new RuntimeException();
        }

        @Override
        public long length() {
            return 0;
        }
    }
}