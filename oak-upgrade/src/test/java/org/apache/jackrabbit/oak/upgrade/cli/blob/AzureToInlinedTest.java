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

import static org.junit.Assert.assertEquals;

import ch.qos.logback.classic.Level;
import java.io.IOException;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.blob.UploadStagingCache;
import org.apache.jackrabbit.oak.upgrade.cli.AbstractOak2OakTest;
import org.apache.jackrabbit.oak.upgrade.cli.container.AzureDataStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.BlobStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class AzureToInlinedTest extends AbstractOak2OakTest {

    private static final String AZURE_PROPERTIES = System.getProperty("azure.properties");

    private final NodeStoreContainer source;

    private final NodeStoreContainer destination;

    private final BlobStoreContainer sourceBlob;

    private LogCustomizer customLogs;

    public AzureToInlinedTest() throws IOException {
        Assume.assumeTrue(AZURE_PROPERTIES != null && !AZURE_PROPERTIES.isEmpty());
        sourceBlob = new AzureDataStoreContainer(AZURE_PROPERTIES);
        source = new SegmentTarNodeStoreContainer(sourceBlob);
        destination = new SegmentTarNodeStoreContainer();
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
        return new String[]{"--copy-binaries", "--src-azuredatastore", sourceBlob.getDescription(),
            "--src-azureconfig",
            AZURE_PROPERTIES, source.getDescription(), destination.getDescription()};
    }

    @Override
    protected boolean supportsCheckpointMigration() {
        return true;
    }

    @Before
    public void prepare() throws Exception {
        // Capture logs
        customLogs = LogCustomizer
            .forLogger(UploadStagingCache.class.getName())
            .enable(Level.INFO)
            .filter(Level.INFO)
            .contains("Uploads in progress on close [0]")
            .create();
        customLogs.starting();
        super.prepare();
    }

    /**
     * Tests whether all the s3 uploads finished
     */
    @Test
    public void testAsyncUploadFinished() {
        assertEquals(1, customLogs.getLogs().size());
        customLogs.finished();
    }

}
