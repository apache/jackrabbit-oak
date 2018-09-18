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

import java.io.IOException;

import org.apache.jackrabbit.oak.upgrade.cli.AbstractOak2OakTest;
import org.apache.jackrabbit.oak.upgrade.cli.container.BlobStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.FileBlobStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.S3DataStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;
import org.junit.Assume;

public class FbsToS3Test extends AbstractOak2OakTest {

    private static final String S3_PROPERTIES = System.getProperty("s3.properties");

    private final BlobStoreContainer sourceBlob;

    private final BlobStoreContainer destinationBlob;

    private final NodeStoreContainer source;

    private final NodeStoreContainer destination;

    public FbsToS3Test() throws IOException {
        Assume.assumeTrue(S3_PROPERTIES != null && !S3_PROPERTIES.isEmpty());
        sourceBlob = new FileBlobStoreContainer();
        destinationBlob = new S3DataStoreContainer(S3_PROPERTIES);
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
        return new String[] { "--copy-binaries", "--src-fileblobstore", sourceBlob.getDescription(), "--s3datastore",
                destinationBlob.getDescription(), "--s3config", S3_PROPERTIES, source.getDescription(),
                destination.getDescription() };
    }

    @Override
    protected boolean supportsCheckpointMigration() {
        return true;
    }
}
