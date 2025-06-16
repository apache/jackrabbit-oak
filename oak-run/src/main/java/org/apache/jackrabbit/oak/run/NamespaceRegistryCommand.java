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
import org.apache.jackrabbit.oak.api.ContentRepository;
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
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.SimpleCredentials;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class NamespaceRegistryCommand implements Command {

    public static final String NAME = "namespace-registry";

    private static final Logger LOG = LoggerFactory.getLogger(NamespaceRegistryCommand.class);
    private final String SUMMARY = "Provides commands to analyse the integrity of the namespace registry and repair it if necessary.";

    private Options opts;
    private NamespaceRegistryOptions namespaceRegistryOpts;

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(SUMMARY);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(NamespaceRegistryOptions.FACTORY);
        opts.parseAndConfigure(parser, args);

        namespaceRegistryOpts = opts.getOptionBean(NamespaceRegistryOptions.class);

        try (Closer closer = Utils.createCloserWithShutdownHook()) {

            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
            closer.register(fixture);

            if (!checkParameters(namespaceRegistryOpts, opts, fixture, parser)) {
                return;
            }
            doExecute(fixture, namespaceRegistryOpts, opts, closer);
        } catch (Throwable e) {
            LOG.error("Error occurred while performing namespace registry operation", e);
            e.printStackTrace(System.err);
        }
    }

    private static boolean checkParameters(NamespaceRegistryOptions namespaceRegistryOptions,
                                           Options opts,
                                           NodeStoreFixture fixture,
                                           OptionParser parser) throws IOException {

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

    private void doExecute(NodeStoreFixture fixture, NamespaceRegistryOptions namespaceRegistryOptions, Options opts, Closer closer)
            throws Exception {

        boolean analyse = namespaceRegistryOptions.analyse();
        boolean fix = namespaceRegistryOptions.fix();
        List<String> mappings = namespaceRegistryOptions.mappings();
        //TODO decide whether admin credentials should be required for this command
        NodeStore store = fixture.getStore();
        NodeState rootState = store.getRoot();
        Oak oak = new Oak(store).with(SecurityProviderBuilder.newBuilder().build());
        //Oak oak = new Oak(fixture.getStore()).with(new OpenSecurityProvider());
        ContentRepository cr = oak.createContentRepository();
        ContentSession contentSession = cr.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        Root root = contentSession.getLatestRoot();
        ReadWriteNamespaceRegistry namespaceRegistry = new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };
        if (analyse || fix) {
            NamespaceRegistryModel registryModel = namespaceRegistry.createNamespaceRegistryModel(root);
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
                    store.merge(rootState.builder(), EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    repaired.dump();
                } else {
                    registryModel.dump();
                }
            } else {
                registryModel.dump();
            }
        } else {
            System.err.println("No action specified. Use --analyse to check the integrity of the namespace registry. Use --fix to repair it if necessary and possible.");
        }
    }
}
