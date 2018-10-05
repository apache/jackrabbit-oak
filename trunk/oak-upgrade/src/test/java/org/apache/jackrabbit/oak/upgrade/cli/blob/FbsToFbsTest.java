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
package org.apache.jackrabbit.oak.upgrade.cli.blob;

import org.apache.jackrabbit.oak.upgrade.cli.AbstractOak2OakTest;
import org.apache.jackrabbit.oak.upgrade.cli.container.BlobStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.FileBlobStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;

import java.io.IOException;

public class FbsToFbsTest extends AbstractOak2OakTest {

    private final BlobStoreContainer sourceBlob;

    private final BlobStoreContainer destinationBlob;

    private final NodeStoreContainer source;

    private final NodeStoreContainer destination;

    public FbsToFbsTest() throws IOException {
        sourceBlob = new FileBlobStoreContainer();
        destinationBlob = new FileBlobStoreContainer();
        source = new SegmentTarNodeStoreContainer(sourceBlob);
        destination = new SegmentTarNodeStoreContainer(destinationBlob);
    }

    @Override
    protected NodeStoreContainer getSourceContainer() {
        return source;
    }

    @Override
    protected NodeStoreContainer getDestinationContainer() {
        return destination;
    }

    @Override
    protected String[] getArgs() {
        return new String[] { "--copy-binaries", "--src-fileblobstore", sourceBlob.getDescription(), "--fileblobstore",
                destinationBlob.getDescription(), source.getDescription(), destination.getDescription() };
    }

    @Override
    protected boolean supportsCheckpointMigration() {
        return true;
    }
}
