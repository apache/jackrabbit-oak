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
package org.apache.jackrabbit.oak.upgrade.cli;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.regex.Pattern;

import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;

import joptsimple.OptionParser;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.ConfigurationException;
import org.apache.jackrabbit.core.fs.FileSystemException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositorySidegrade;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade;
import org.apache.jackrabbit.oak.upgrade.cli.parser.ArgumentParser;
import org.apache.jackrabbit.oak.upgrade.cli.parser.CliArgumentException;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationOptions;
import org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

@SuppressWarnings("restriction")
public class OakUpgrade {

    private static final Logger log = LoggerFactory.getLogger(OakUpgrade.class);

    public static void main(String... args) throws IOException {
        ArgumentParser argumentParser = parse(OptionParserFactory.create(), args);
        if (argumentParser == null) {
            return;
        }
        migrate(argumentParser);
    }

    protected static void migrate(ArgumentParser argumentParser) throws IOException {
        MigrationOptions options = argumentParser.getOptions();
        StoreArguments stores = argumentParser.getStoreArguments();
        final Closer closer = Closer.create();
        handleSigInt(closer);
        try {
            if (stores.getSrcStore().isJcr2()) {
                upgrade(stores, options, closer);
            } else {
                sidegrade(stores, options, closer);
            }
        } catch (Throwable t) {
            if (t instanceof RepositoryException && t.getCause() != null) {
                log.error("OakUpgrade failed", t.getCause());
            }
            throw closer.rethrow(t);
        } finally {
            closer.close();
        }
        if (stores.isInPlaceUpgrade()) {
            moveToCrx2Dir(stores.getSrcPaths()[0]);
        }
    }

    protected static ArgumentParser parse(OptionParser op, String... args) {
        try {
            return ArgumentParser.parse(args, op);
        } catch (Exception e) {
            System.exit(getReturnCode(e));
            return null;
        }
    }
    
    protected static void handleSigInt(final Closer closer) {
        final SignalHandler handler = new SignalHandler() {
            @Override
            public void handle(Signal signal) {
                try {
                    closer.close();
                } catch (IOException e) {
                    log.error("Can't close", e);
                }
                System.exit(0);
            }
        };
        Signal.handle(new Signal("INT"), handler);
    }

    protected static void sidegrade(StoreArguments stores, MigrationOptions options, Closer closer)
            throws IOException, RepositoryException {
        final BlobStore srcBlobStore = stores.getSrcBlobStore().create(closer);
        final NodeStore srcStore = stores.getSrcStore().create(srcBlobStore, closer);
        final BlobStore dstBlobStore;
        if (options.isCopyBinariesByReference()) {
            dstBlobStore = srcBlobStore;
        } else {
            dstBlobStore = stores.getDstBlobStore().create(closer);
        }
        final NodeStore dstStore = stores.getDstStore().create(dstBlobStore, closer);

        final RepositorySidegrade sidegrade = new RepositorySidegrade(srcStore, dstStore);
        sidegrade.setCopyVersions(options.getCopyVersions());
        sidegrade.setCopyOrphanedVersions(options.getCopyOrphanedVersions());
        if (options.getIncludePaths() != null) {
            sidegrade.setIncludes(options.getIncludePaths());
        }
        if (options.getExcludePaths() != null) {
            sidegrade.setExcludes(options.getExcludePaths());
        }
        if (options.getMergePaths() != null) {
            sidegrade.setMerges(options.getMergePaths());
        }
        sidegrade.copy();
    }

    private static void upgrade(StoreArguments stores, MigrationOptions options, Closer closer)
            throws ConfigurationException, IOException, FileSystemException, RepositoryException {
        final RepositoryContext src = stores.getSrcStore().create(closer);
        final BlobStore srcBlobStore = new DataStoreBlobStore(src.getDataStore());
        final BlobStore dstBlobStore;
        if (options.isCopyBinariesByReference()) {
            dstBlobStore = srcBlobStore;
        } else {
            dstBlobStore = stores.getDstBlobStore().create(closer);
        }
        final NodeStore dstStore = stores.getDstStore().create(dstBlobStore, closer);
        RepositoryUpgrade upgrade = createRepositoryUpgrade(src, dstStore, options);
        upgrade.copy(createCompositeInitializer(options));
    }

    protected static void moveToCrx2Dir(String repositoryDirPath) {
        // backup old crx2 files when doing an in-place upgrade
        final File repositoryDir = new File(repositoryDirPath);
        final File crx2 = new File(repositoryDir, "crx2");
        log.info("Moving existing repository under {}", crx2.getAbsolutePath());
        crx2.mkdir();
        final Pattern pattern = Pattern.compile("crx2|segmentstore");
        for (File file : repositoryDir.listFiles()) {
            final String name = file.getName();
            if (!pattern.matcher(name).matches()) {
                file.renameTo(new File(crx2, name));
            }
        }
    }

    protected static RepositoryUpgrade createRepositoryUpgrade(RepositoryContext source, NodeStore store,
            MigrationOptions options) {
        RepositoryUpgrade upgrade = new RepositoryUpgrade(source, store);
        if (source.getDataStore() != null && options.isCopyBinariesByReference()) {
            upgrade.setCopyBinariesByReference(true);
        }
        upgrade.setCopyVersions(options.getCopyVersions());
        upgrade.setCopyOrphanedVersions(options.getCopyOrphanedVersions());
        if (options.getIncludePaths() != null) {
            upgrade.setIncludes(options.getIncludePaths());
        }
        if (options.getExcludePaths() != null) {
            upgrade.setExcludes(options.getExcludePaths());
        }
        upgrade.setSkipOnError(!options.isFailOnError());
        upgrade.setEarlyShutdown(options.isEarlyShutdown());
        ServiceLoader<CommitHook> loader = ServiceLoader.load(CommitHook.class);
        Iterator<CommitHook> iterator = loader.iterator();
        ImmutableList.Builder<CommitHook> builder = ImmutableList.<CommitHook> builder().addAll(iterator);
        upgrade.setCustomCommitHooks(builder.build());
        return upgrade;
    }

    protected static RepositoryInitializer createCompositeInitializer(MigrationOptions options) {
        ServiceLoader<RepositoryInitializer> loader = ServiceLoader.load(RepositoryInitializer.class);
        List<RepositoryInitializer> initializers = ImmutableList.<RepositoryInitializer> builder()
                .addAll(loader.iterator()).build();
        return new CompositeInitializer(initializers);
    }

    private static int getReturnCode(Exception e) {
        if (e.getMessage() != null) {
            System.err.println(e.getMessage());
        }
        if (e instanceof CliArgumentException) {
            return ((CliArgumentException) e).getExitCode();
        } else {
            return 1;
        }
    }
}