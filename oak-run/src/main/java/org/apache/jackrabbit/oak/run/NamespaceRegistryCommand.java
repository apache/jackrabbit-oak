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
package org.apache.jackrabbit.oak.run;

import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.pio.Closer;
import org.apache.jackrabbit.oak.plugins.name.NamespaceRegistryModel;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Command to analyze and repair the namespace registry in an Oak repository ({@link NamespaceRegistryModel}).
 * Possible options are: --analyse, --fix, and --mappings, which will execute corresponding operations on
 * the namespace registry.
 * <p>
 * --analyse executes an operation that will print the current consistency state of the namespace registry to
 * the console. If the namespace registry is inconsistent and fixable, it will also perform a dry run of the
 * --fix operation and print the result to the console.
 * <p>
 * --fix executes an operation that will attempt to repair an inconsistent the namespace registry.
 * <p>
 * --mappings is an option for both operations, allowing to specify additional namespace mappings in
 * the format "prefix=uri", which will be applied during the operation.
 */
public class NamespaceRegistryCommand implements Command {

    public static final String NAME = "namespace-registry";

    private static final Logger LOG = LoggerFactory.getLogger(NamespaceRegistryCommand.class);
    private static final String SUMMARY = "Provides commands to analyse the integrity of the namespace registry and repair it if necessary.";

    private final OptionParser parser = new OptionParser();

    @Override
    public void execute(String... args) throws Exception {
        Options opts = getOptions(args);
        NamespaceRegistryOptions namespaceRegistryOpts = opts.getOptionBean(NamespaceRegistryOptions.class);
        try (Closer closer = Utils.createCloserWithShutdownHook()) {

            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
            closer.register(fixture);

            if (!checkParameters(namespaceRegistryOpts, fixture)) {
                return;
            }
            doExecute(fixture, namespaceRegistryOpts);
        } catch (Exception e) {
            LOG.error("Error occurred while performing namespace registry operation", e);
            e.printStackTrace(System.err);
            throw e;
        }
    }

    Options getOptions(String... args) throws IOException {
        Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(SUMMARY);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(NamespaceRegistryOptions.FACTORY);
        opts.parseAndConfigure(parser, args);
        return opts;
    }

    private boolean checkParameters(NamespaceRegistryOptions namespaceRegistryOptions, NodeStoreFixture fixture)
            throws IOException {
        if (!namespaceRegistryOptions.anyActionSelected()) {
            LOG.info("No actions specified");
            parser.printHelpOn(System.out);
            return false;
        } else if (fixture.getStore() == null) {
            LOG.info("No NodeStore specified");
            parser.printHelpOn(System.out);
            return false;
        }
        return true;
    }

    private void doExecute(NodeStoreFixture fixture, NamespaceRegistryOptions namespaceRegistryOptions)
            throws IOException, RepositoryException, CommitFailedException {
        boolean analyse = namespaceRegistryOptions.analyse();
        boolean fix = namespaceRegistryOptions.fix();
        List<String> mappings = namespaceRegistryOptions.mappings();
        Oak oak = new Oak(fixture.getStore()).with(new OpenSecurityProvider());
        try (ContentSession contentSession = oak.createContentSession()) {
            Root root = contentSession.getLatestRoot();
            ReadWriteNamespaceRegistry namespaceRegistry = new ReadWriteNamespaceRegistry(root) {
                @Override
                protected Root getWriteRoot() {
                    return root;
                }
            };
            if (analyse || fix) {
                NamespaceRegistryModel registryModel = NamespaceRegistryModel.create(root);
                if (fix) {
                    Map<String, String> additionalMappings = new HashMap<>();
                    if (mappings != null) {
                        for (String mapping : mappings) {
                            String[] parts = mapping.split("=");
                            if (parts.length != 2) {
                                System.err.println("Invalid mapping: " + mapping);
                                return;
                            }
                            additionalMappings.put(parts[0].trim(), parts[1].trim());
                        }
                    }
                    registryModel = registryModel.setMappings(additionalMappings);
                    if (registryModel.isConsistent() && additionalMappings.isEmpty()) {
                        System.out.println("The namespace registry is already consistent. No action is required.");
                    } else if (registryModel.isFixable()) {
                        registryModel.dump(System.out);
                        System.out.println();
                        System.out.println("Now fixing the registry.");
                        System.out.println();
                        System.out.flush();
                        NamespaceRegistryModel repaired = registryModel.tryRegistryRepair();
                        if (repaired == null) {
                            System.out.println("An unknown error has occurred. No changes have been made to the namespace registry.");
                            return;
                        }
                        repaired.apply(root);
                        root.commit();
                        repaired.dump();
                    } else {
                        registryModel.dump();
                    }
                } else {
                    if (registryModel == null) {
                        System.out.println("There is no namespace registry in the repository.");
                    } else {
                        registryModel.dump();
                    }
                }
            } else {
                System.err.println("No action specified. Use --analyse to check the integrity of the namespace registry. Use --fix to repair it if necessary and possible.");
            }
        }
    }
}
