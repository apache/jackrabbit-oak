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
package org.apache.jackrabbit.oak.run;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SimpleFlatFileUtil;
import org.apache.jackrabbit.oak.run.cli.BlobStoreOptions;
import org.apache.jackrabbit.oak.run.cli.BlobStoreOptions.Type;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.cli.OptionsBean;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * The flatfile command is an extract of the ability to create a filefile from
 * the index command. Other than the origin, the flatfile command doesn't
 * require any checkpoint nor index name.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class FlatFileCommand implements Command {

    public static final String NAME = "flatfile";
    private final String summary = "Provides flatfile operations";

    public static class FlatFileOptions implements OptionsBean {

        private final OptionSpec<File> outFileOpt;
        private final OptionSpec<Void> flatfile;
        protected OptionSet options;
        protected final Set<OptionSpec> actionOpts;
        private final Set<String> operationNames;

        public FlatFileOptions(OptionParser parser) {
            flatfile = parser.accepts("flatfile",
                    "Create a flatfile based on head of a repository");
            outFileOpt = parser.accepts("out", "Name of the flatfile to create")
                    .withRequiredArg().ofType(File.class).defaultsTo(new File("temp"));
            // Set of options which define action
            actionOpts = Set.of(flatfile);
            operationNames = collectionOperationNames(actionOpts);
        }

        @Override
        public void configure(OptionSet options) {
            this.options = options;
        }

        @Override
        public String title() {
            return "";
        }

        @Override
        public String description() {
            return "The flatfile command supports creation of a flatfile for current head of a repository.";
        }

        @Override
        public int order() {
            return 50;
        }

        @Override
        public Set<String> operationNames() {
            return operationNames;
        }

        public File getOutFile() throws IOException {
            File outFile = outFileOpt.value(options);
            FileUtils.forceMkdir(outFile.getParentFile());
            return outFile;
        }

        private static Set<String> collectionOperationNames(Set<OptionSpec> actionOpts) {
            Set<String> result = new HashSet<>();
            for (OptionSpec spec : actionOpts) {
                result.addAll(spec.options());
            }
            return result;
        }
    }

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.registerOptionsFactory(FlatFileOptions::new);
        opts.registerOptionsFactory(BlobStoreOptions::new);
        opts.parseAndConfigure(parser, args);
        BlobStoreOptions bsOpts = opts.getOptionBean(BlobStoreOptions.class);
        Type bsType = bsOpts.getBlobStoreType();
        if (bsType == Type.NONE) {
            System.err.println("BlobStore args missing.");
            System.err.println("Hint: consider use fake one via: --fake-ds-path=.");
            System.exit(1);
        }
        FlatFileOptions ffOpts = opts.getOptionBean(FlatFileOptions.class);
        File out = ffOpts.getOutFile();

        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts, true)) {
            NodeStore store = fixture.getStore();
            NodeState root = store.getRoot();
            SimpleFlatFileUtil.createFlatFileFor(root, out);
        }
    }

}