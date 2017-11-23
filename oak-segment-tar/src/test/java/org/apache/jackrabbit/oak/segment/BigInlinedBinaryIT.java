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
 *
 */

package org.apache.jackrabbit.oak.segment;

import static java.lang.Long.signum;
import static java.lang.System.getProperty;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.segment.ListRecord.LEVEL_SIZE;
import static org.apache.jackrabbit.oak.segment.SegmentStream.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration test trying to inline a large (67GB) binary.
 * Skipped unless -Dtest=BigInlinedBinaryIT is specified.
 */
public class BigInlinedBinaryIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Before
    public void setup() {
        Assume.assumeTrue(BigInlinedBinaryIT.class.getSimpleName().equals(getProperty("test")));
    }

    @Test
    public void largeBlob()
    throws IOException, CommitFailedException, InvalidFileStoreVersionException {
        try (FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setChildNode("node").setProperty("blob",
                    createBlob((LEVEL_SIZE * LEVEL_SIZE * LEVEL_SIZE + 1L) * BLOCK_SIZE));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            Blob blob = nodeStore.getRoot().getChildNode("node").getProperty("blob").getValue(BINARY);

            byte[] buffer = new byte[4096];
            blob.getNewStream().read(buffer, 0, buffer.length);
        }
    }

    @Nonnull
    private static Blob createBlob(long blobSize) {
        return new Blob() {
                @Nonnull
                @Override
                public InputStream getNewStream() {
                    return new InputStream() {
                        long pos = 0;
                        @Override
                        public int read() {
                            return signum(blobSize - ++pos);
                        }
                    };
                }

                @Override
                public long length() {
                    return blobSize;
                }

                @CheckForNull
                @Override
                public String getReference() {
                    return null;
                }

                @CheckForNull
                @Override
                public String getContentIdentity() {
                    return null;
                }
            };
    }

}
