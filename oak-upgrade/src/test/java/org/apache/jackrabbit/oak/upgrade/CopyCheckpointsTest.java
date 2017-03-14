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
package org.apache.jackrabbit.oak.upgrade;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.AbstractOak2OakTest;
import org.apache.jackrabbit.oak.upgrade.cli.OakUpgrade;
import org.apache.jackrabbit.oak.upgrade.cli.container.BlobStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.FileDataStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.parser.CliArgumentException;
import org.apache.jackrabbit.oak.upgrade.cli.parser.DatastoreArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationCliArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationOptions;
import org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class CopyCheckpointsTest extends AbstractOak2OakTest {

    private enum Result {
        EXCEPTION, CHECKPOINTS_MISSING, CHECKPOINTS_COPIED
    }

    private static final Logger log = LoggerFactory.getLogger(CopyCheckpointsTest.class);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws IOException {
        List<Object[]> params = new ArrayList<Object[]>();

        BlobStoreContainer blob = new FileDataStoreContainer();
        params.add(new Object[]{
                "Fails on missing blobstore",
                new SegmentNodeStoreContainer(blob),
                new SegmentNodeStoreContainer(blob),
                asList(),
                Result.EXCEPTION
        });
        params.add(new Object[]{
                "Suppress the warning",
                new SegmentNodeStoreContainer(blob),
                new SegmentNodeStoreContainer(blob),
                asList("--skip-checkpoints"),
                Result.CHECKPOINTS_MISSING
        });
        params.add(new Object[]{
                "Source data store defined, checkpoints migrated",
                new SegmentTarNodeStoreContainer(blob),
                new SegmentTarNodeStoreContainer(blob),
                asList("--src-datastore=" + blob.getDescription()),
                Result.CHECKPOINTS_COPIED
        });
        return params;
    }

    private final NodeStoreContainer source;

    private final NodeStoreContainer destination;

    private final List<String> args;

    private final Result expectedResult;

    public CopyCheckpointsTest(String name, NodeStoreContainer source, NodeStoreContainer destination, List<String> args, Result expectedResult) throws IOException, CliArgumentException {
        this.source = source;
        this.destination = destination;
        this.args = args;
        this.expectedResult = expectedResult;

        this.source.clean();
        this.destination.clean();
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
        List<String> result = new ArrayList<>(args);
        result.addAll(asList("--disable-mmap", source.getDescription(), destination.getDescription()));
        return result.toArray(new String[result.size()]);
    }

    @Before
    @Override
    public void prepare() throws Exception {
        NodeStore source = getSourceContainer().open();
        try {
            initContent(source);
        } finally {
            getSourceContainer().close();
        }

        String[] args = getArgs();
        log.info("oak2oak {}", Joiner.on(' ').join(args));
        try {
            MigrationCliArguments cliArgs = new MigrationCliArguments(OptionParserFactory.create().parse(args));
            MigrationOptions options = new MigrationOptions(cliArgs);
            StoreArguments stores = new StoreArguments(options, cliArgs.getArguments());
            DatastoreArguments datastores = new DatastoreArguments(options, stores, stores.srcUsesEmbeddedDatastore());
            OakUpgrade.migrate(options, stores, datastores);
        } catch(RuntimeException e) {
            if (expectedResult == Result.EXCEPTION) {
                return;
            } else {
                throw e;
            }
        }
        if (expectedResult == Result.EXCEPTION) {
            fail("Migration should fail");
        }
        createSession();
    }

    @Test
    @Override
    public void validateMigration() throws RepositoryException, IOException, CliArgumentException {
        switch (expectedResult) {
            case CHECKPOINTS_COPIED:
                verifyCheckpoint();
                break;

            case CHECKPOINTS_MISSING:
                verifyEmptyAsync();
                break;
        }
    }
}