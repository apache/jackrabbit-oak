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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.AbstractOak2OakTest;
import org.apache.jackrabbit.oak.upgrade.cli.OakUpgrade;
import org.apache.jackrabbit.oak.upgrade.cli.container.BlobStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.FileDataStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.JdbcNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.parser.CliArgumentException;
import org.apache.jackrabbit.oak.upgrade.cli.parser.DatastoreArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationCliArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationOptions;
import org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;

@RunWith(Parameterized.class)
public class CopyBinariesTest extends AbstractOak2OakTest {

    private static final Logger log = LoggerFactory.getLogger(CopyBinariesTest.class);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws IOException {
        List<Object[]> params = new ArrayList<Object[]>();

        BlobStoreContainer blob = new FileDataStoreContainer();
        BlobStoreContainer blob2 = new FileDataStoreContainer();
        params.add(new Object[]{
                "Copy references, no blobstores defined, segment -> segment",
                new SegmentNodeStoreContainer(blob),
                new SegmentNodeStoreContainer(blob),
                asList(),
                DatastoreArguments.BlobMigrationCase.COPY_REFERENCES
        });
        params.add(new Object[]{
                "Copy references, no blobstores defined, segment-tar -> segment-tar",
                new SegmentTarNodeStoreContainer(blob),
                new SegmentTarNodeStoreContainer(blob),
                asList(),
                DatastoreArguments.BlobMigrationCase.COPY_REFERENCES
        });
        params.add(new Object[]{
                "Copy references, no blobstores defined, segment -> segment-tar",
                new SegmentNodeStoreContainer(blob),
                new SegmentTarNodeStoreContainer(blob),
                asList(),
                DatastoreArguments.BlobMigrationCase.COPY_REFERENCES
        });
        params.add(new Object[]{
                "Copy references, no blobstores defined, document -> segment-tar",
                new JdbcNodeStoreContainer(blob),
                new SegmentNodeStoreContainer(blob),
                asList("--src-user=sa", "--src-password=sa"),
                DatastoreArguments.BlobMigrationCase.COPY_REFERENCES
        });
        params.add(new Object[]{
                "Copy references, no blobstores defined, segment-tar -> document",
                new SegmentTarNodeStoreContainer(blob),
                new JdbcNodeStoreContainer(blob),
                asList("--user=sa", "--password=sa"),
                DatastoreArguments.BlobMigrationCase.UNSUPPORTED
        });
        params.add(new Object[]{
                "Missing source, external destination",
                new SegmentTarNodeStoreContainer(blob),
                new SegmentTarNodeStoreContainer(blob),
                asList("--datastore=" + blob.getDescription()),
                DatastoreArguments.BlobMigrationCase.UNSUPPORTED
        });
        params.add(new Object[]{
                "Copy embedded to embedded, no blobstores defined",
                new SegmentTarNodeStoreContainer(),
                new SegmentTarNodeStoreContainer(),
                asList(),
                DatastoreArguments.BlobMigrationCase.EMBEDDED_TO_EMBEDDED
        });
        params.add(new Object[]{
                "Copy embedded to external, no blobstores defined",
                new SegmentTarNodeStoreContainer(),
                new SegmentTarNodeStoreContainer(blob),
                asList("--datastore=" + blob.getDescription()),
                DatastoreArguments.BlobMigrationCase.EMBEDDED_TO_EXTERNAL
        });
        params.add(new Object[]{
                "Copy references, src blobstore defined",
                new SegmentTarNodeStoreContainer(blob),
                new SegmentTarNodeStoreContainer(blob),
                asList("--src-datastore=" + blob.getDescription()),
                DatastoreArguments.BlobMigrationCase.COPY_REFERENCES
        });
        params.add(new Object[]{
                "Copy external to embedded, src blobstore defined",
                new SegmentTarNodeStoreContainer(blob),
                new SegmentTarNodeStoreContainer(),
                asList("--copy-binaries", "--src-datastore=" + blob.getDescription()),
                DatastoreArguments.BlobMigrationCase.EXTERNAL_TO_EMBEDDED
        });
        params.add(new Object[]{
                "Copy external to external, src blobstore defined",
                new SegmentTarNodeStoreContainer(blob),
                new SegmentTarNodeStoreContainer(blob2),
                asList("--copy-binaries", "--src-datastore=" + blob.getDescription(), "--datastore=" + blob2.getDescription()),
                DatastoreArguments.BlobMigrationCase.EXTERNAL_TO_EXTERNAL
        });
        return params;
    }

    private final NodeStoreContainer source;

    private final NodeStoreContainer destination;

    private final List<String> args;

    private final DatastoreArguments.BlobMigrationCase blobMigrationCase;

    public CopyBinariesTest(String name, NodeStoreContainer source, NodeStoreContainer destination, List<String> args, DatastoreArguments.BlobMigrationCase blobMigrationCase) throws IOException, CliArgumentException {
        this.source = source;
        this.destination = destination;
        this.args = args;
        this.blobMigrationCase = blobMigrationCase;

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
        result.addAll(asList("--disable-mmap", "--skip-checkpoints", source.getDescription(), destination.getDescription()));
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
            assertEquals(blobMigrationCase, datastores.getBlobMigrationCase());
        } catch(CliArgumentException e) {
            if (blobMigrationCase == DatastoreArguments.BlobMigrationCase.UNSUPPORTED) {
                return;
            } else {
                throw e;
            }
        }
        createSession();
    }

    @Test
    @Override
    public void validateMigration() throws RepositoryException, IOException, CliArgumentException {
        if (blobMigrationCase == DatastoreArguments.BlobMigrationCase.UNSUPPORTED) {
            return;
        }
        super.validateMigration();
    }
}