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

import static org.apache.jackrabbit.oak.upgrade.cli.container.MongoNodeStoreContainer.isMongoAvailable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.jackrabbit.oak.upgrade.cli.AbstractOak2OakTest;
import org.apache.jackrabbit.oak.upgrade.cli.container.BlobStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.FileDataStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.MongoNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class MissingBlobStoreTest extends AbstractOak2OakTest {

    private static final Logger log = LoggerFactory.getLogger(MissingBlobStoreTest.class);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<Object[]>();
        BlobStoreContainer blob;

        blob = new FileDataStoreContainer();
        params.add(new Object[] { "Segment -> Segment (FDS)", new SegmentNodeStoreContainer(blob), new SegmentNodeStoreContainer(blob), true });
        params.add(new Object[] { "Segment -> SegmentTar (FDS)", new SegmentNodeStoreContainer(blob), new SegmentTarNodeStoreContainer(blob), true });
        params.add(new Object[] { "SegmentTar -> Segment (FDS)", new SegmentTarNodeStoreContainer(blob), new SegmentNodeStoreContainer(blob), true });
        params.add(new Object[] { "SegmentTar -> SegmentTar (FDS)", new SegmentTarNodeStoreContainer(blob), new SegmentTarNodeStoreContainer(blob), true });
        try {
            if (isMongoAvailable()) {
                params.add(new Object[] { "Mongo -> Mongo (FDS)",
                        new MongoNodeStoreContainer(blob),
                        new MongoNodeStoreContainer(blob), false });
                params.add(new Object[] { "Mongo -> Segment (FDS)",
                        new MongoNodeStoreContainer(blob),
                        new SegmentNodeStoreContainer(blob), false });
            }
        } catch (IOException e) {
            log.error("Can't create Mongo -> Mongo case", e);
        }
        return params;
    }

    private final NodeStoreContainer source;

    private final NodeStoreContainer destination;

    private final boolean supportsCheckpoint;

    public MissingBlobStoreTest(String name, NodeStoreContainer source, NodeStoreContainer destination, boolean supportsCheckpoint) {
        this.source = source;
        this.destination = destination;
        this.supportsCheckpoint = supportsCheckpoint;
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
        return new String[] { "--missingblobstore", source.getDescription(), destination.getDescription() };
    }

    protected boolean supportsCheckpointMigration() {
        return supportsCheckpoint;
    }
}
