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

package org.apache.jackrabbit.oak.exporter;

import java.io.File;

import com.google.common.base.Stopwatch;
import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;

public class NodeStateExportCommand implements Command {
    public static final String NAME = "export";

    private final String summary = "Exports NodeState as json";

    @Override
    public void execute(String... args) throws Exception {
        Stopwatch w = Stopwatch.createStarted();
        OptionParser parser = new OptionParser();

        Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.registerOptionsFactory(ExportOptions.FACTORY);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);

        opts.parseAndConfigure(parser, args);

        ExportOptions eo = opts.getOptionBean(ExportOptions.class);

        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
            NodeStateSerializer serializer = new NodeStateSerializer(fixture.getStore().getRoot());

            serializer.setDepth(eo.getDepth());
            serializer.setPath(eo.getPath());
            serializer.setFilter(eo.getFilter());
            serializer.setFilterFile(eo.getFilterFile());
            serializer.setFormat(eo.getFormat());
            serializer.setMaxChildNodes(eo.getMaxChildNodes());
            serializer.setPrettyPrint(eo.isPrettyPrint());
            serializer.setSerializeBlobContent(eo.includeBlobs());

            File dir = eo.getOutDir();

            serializer.serialize(dir);

            System.out.printf("Export the nodes under path [%s] to file [%s] in %s%n",
                    eo.getPath(), dir.getAbsolutePath(), w);

        }
    }
}
