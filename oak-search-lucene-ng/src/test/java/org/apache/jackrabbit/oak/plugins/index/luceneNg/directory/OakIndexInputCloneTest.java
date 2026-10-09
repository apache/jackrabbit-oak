/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

public class OakIndexInputCloneTest {

    @Test
    public void clonedInputMustNotShareReadPositionWithOriginal() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();

        OakDirectory writeDirectory = new OakDirectory(builder, "test-index", false);
        try (IndexOutput out = writeDirectory.createOutput("data.bin", IOContext.DEFAULT)) {
            byte[] as = new byte[100];
            Arrays.fill(as, (byte) 'A');
            byte[] bs = new byte[100];
            Arrays.fill(bs, (byte) 'B');
            out.writeBytes(as, 0, as.length);
            out.writeBytes(bs, 0, bs.length);
        }
        writeDirectory.close();

        OakDirectory readDirectory = new OakDirectory(builder, "test-index", true);
        IndexInput original = readDirectory.openInput("data.bin", IOContext.DEFAULT);
        original.seek(10); // inside the 'A' region

        IndexInput clone = original.clone();
        clone.seek(150); // inside the 'B' region — must not affect `original`

        byte fromOriginal = original.readByte();
        assertEquals("cloning must give the clone its own read cursor; "
                + "moving the clone's position must not move the original's",
                (byte) 'A', fromOriginal);

        byte fromClone = clone.readByte();
        assertEquals((byte) 'B', fromClone);

        original.close();
        clone.close();
        readDirectory.close();
    }
}
