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
import java.util.Collections;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.cli.OptionsBean;
import org.apache.jackrabbit.oak.run.cli.OptionsBeanFactory;

import static java.util.Arrays.asList;


class ExportOptions implements OptionsBean {

    public static final OptionsBeanFactory FACTORY = ExportOptions::new;

    private String defaultFilter = "{\"properties\":[\"*\", \"-:childOrder\"]}";
    private final OptionSpec<File> filterFileOpt;
    private final OptionSpec<File> outDirOpt;
    private final OptionSpec<String> filter;
    private final OptionSpec<String> path;
    private final OptionSpec<String> format;
    private final OptionSpec<Boolean> serializeBlobContent;
    private final OptionSpec<Boolean> prettyPrint;
    private final OptionSpec<Integer> depth;
    private final OptionSpec<Integer> maxChildNodes;
    private OptionSet options;


    public ExportOptions(OptionParser parser){
        filterFileOpt = parser.accepts("filter-file", "Filter file which contains the filter json expression")
                .withRequiredArg().ofType(File.class);
        outDirOpt = parser.acceptsAll(asList("o", "out"), "Output directory where the exported json and blobs are stored")
                .withRequiredArg().ofType(File.class).defaultsTo(new File("."));

        filter = parser.acceptsAll(asList("f", "filter"), "Filter expression as json to filter out which " +
                "nodes and properties are included in exported file")
                .withRequiredArg().ofType(String.class).defaultsTo(defaultFilter);

        path = parser.acceptsAll(asList("p", "path"), "Repository path to export")
                .withRequiredArg().ofType(String.class).defaultsTo("/");
        format = parser.accepts("format", "Export format 'json' or 'txt'")
                .withRequiredArg().ofType(String.class).defaultsTo("json");

        serializeBlobContent = parser.acceptsAll(asList("b", "blobs"), "Export blobs also. " +
                "By default blobs are not exported")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);

        prettyPrint = parser.accepts("pretty", "Pretty print the json output")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);

        depth = parser.acceptsAll(asList("d", "depth"), "Max depth to include in output")
                .withOptionalArg().ofType(Integer.class).defaultsTo(Integer.MAX_VALUE);

        maxChildNodes = parser.acceptsAll(asList("n", "max-child-nodes"), "Maximum number of child nodes " +
                "to include for a any parent")
                .withOptionalArg().ofType(Integer.class).defaultsTo(Integer.MAX_VALUE);

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
        return "The export command supports exporting nodes from a repository in json. It also provide options " +
                "to export the blobs which are stored in FileDataStore format";
    }

    @Override
    public int order() {
        return 50;
    }

    @Override
    public Set<String> operationNames() {
        return Collections.emptySet();
    }

    //~-------------------------------------------------------------


    public File getFilterFile() {
        return filterFileOpt.value(options);
    }

    public File getOutDir() {
        return outDirOpt.value(options);
    }

    public String getFilter() {
        return filter.value(options);
    }

    public String getPath() {
        return path.value(options);
    }

    public NodeStateSerializer.Format getFormat() {
        String fmt = format.value(options);
        return NodeStateSerializer.Format.valueOf(fmt.toUpperCase());
    }

    public boolean includeBlobs() {
        return serializeBlobContent.value(options);
    }

    public boolean isPrettyPrint() {
        return prettyPrint.value(options);
    }

    public int getDepth() {
        return depth.value(options);
    }

    public int getMaxChildNodes() {
        return maxChildNodes.value(options);
    }
}
