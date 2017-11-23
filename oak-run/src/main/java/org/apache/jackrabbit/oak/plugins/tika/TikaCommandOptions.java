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

package org.apache.jackrabbit.oak.plugins.tika;

import java.io.File;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.cli.OptionsBean;
import org.apache.jackrabbit.oak.run.cli.OptionsBeanFactory;

public class TikaCommandOptions implements OptionsBean {
    public static final String NAME = "tika";

    public static final OptionsBeanFactory FACTORY = TikaCommandOptions::new;

    private final OptionSpec<String> pathOpt;
    private final OptionSpec<File> dataFileSpecOpt;
    private final OptionSpec<File> tikaConfigSpecOpt;
    private final OptionSpec<File> storeDirSpecOpt;
    private final OptionSpec<Integer> poolSizeOpt;

    private final OptionSpec<Void> reportAction;
    private final OptionSpec<Void> generateAction;
    private final OptionSpec<Void> extractAction;

    private final Set<String> operationNames;

    private OptionSet options;

    public TikaCommandOptions(OptionParser parser) {
        pathOpt = parser
                .accepts("path", "Path in repository under which the binaries would be searched")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("/");

        dataFileSpecOpt = parser
                .accepts("data-file", "Data file in csv format containing the binary metadata")
                .withRequiredArg()
                .ofType(File.class)
                .defaultsTo(new File("oak-binary-stats.csv"));

        tikaConfigSpecOpt = parser
                .accepts("tika-config", "Tika config file path")
                .withRequiredArg()
                .ofType(File.class);

        storeDirSpecOpt = parser
                .accepts("store-path", "Path of directory used to store extracted text content")
                .withRequiredArg()
                .ofType(File.class);

        poolSizeOpt = parser
                .accepts("pool-size", "Size of the thread pool used to perform text extraction. Defaults " +
                        "to number of cores on the system")
                .withRequiredArg()
                .ofType(Integer.class);

        reportAction = parser.accepts("report", "Generates a summary report based on the csv file");
        generateAction = parser.accepts("generate", "Generates the CSV file required for 'extract' and 'report' actions");
        extractAction = parser.accepts("extract", "Performs the text extraction based on the csv file");

        operationNames = ImmutableSet.of("report", "generate", "extract");
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
        return "The tika command supports following operations. All operations connect to repository in read only mode. \n" +
                "Use of one of the supported actions like --report, --generate, --extract etc. ";
    }

    @Override
    public int order() {
        return 50;
    }

    @Override
    public Set<String> operationNames() {
        return operationNames;
    }

    public String getPath() {
        return pathOpt.value(options);
    }

    public File getDataFile() {
        return dataFileSpecOpt.value(options);
    }

    public File getTikaConfig() {
        return tikaConfigSpecOpt.value(options);
    }

    public File getStoreDir() {
        return storeDirSpecOpt.value(options);
    }

    public boolean isPoolSizeDefined() {
        return options.has(poolSizeOpt);
    }

    public int getPoolSize() {
        return poolSizeOpt.value(options);
    }

    public boolean report() {
        //The non option mode is for comparability support with previous versions
        return options.has(reportAction) || hasNonOption("report");
    }

    public boolean generate() {
        return options.has(generateAction) || hasNonOption("generate");
    }

    public boolean extract() {
        return options.has(extractAction) || hasNonOption("extract");
    }

    public OptionSpec<File> getDataFileSpecOpt() {
        return dataFileSpecOpt;
    }

    public OptionSpec<File> getStoreDirSpecOpt() {
        return storeDirSpecOpt;
    }

    private boolean hasNonOption(String name) {
        return options.nonOptionArguments().contains(name);
    }
}
