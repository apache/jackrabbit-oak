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

import com.google.common.collect.ImmutableSet;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.run.cli.OptionsBean;
import org.apache.jackrabbit.oak.run.cli.OptionsBeanFactory;

public class DataStoreOptions implements OptionsBean {

    public static final OptionsBeanFactory FACTORY = DataStoreOptions::new;

    private final OptionSpec<File> workDirOpt;
    private final OptionSpec<File> outputDirOpt;
    private final OptionSpec<Boolean> collectGarbage;
    private final OptionSpec<Void> consistencyCheck;
    private final OptionSpec<Integer> batchCount;
    private OptionSet options;
    private final Set<OptionSpec> actionOpts;
    private final Set<String> operationNames;
    private final OptionSpec<Long> blobGcMaxAgeInSecs;
    private final OptionSpec<Void> verbose;
    private final OptionSpec<Boolean> resetLoggingConfig;
    private OptionSpec<String> exportMetrics;

    public DataStoreOptions(OptionParser parser) {
        collectGarbage = parser.accepts("collect-garbage",
            "Performs DataStore Garbage Collection on the repository/datastore defined. An option boolean specifying "
                + "'markOnly' required if only mark phase of garbage collection is to be executed")
            .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);

        consistencyCheck =
            parser.accepts("check-consistency", "Performs a consistency check on the repository/datastore defined");

        blobGcMaxAgeInSecs = parser.accepts("max-age", "")
            .withRequiredArg().ofType(Long.class).defaultsTo(86400L);
        batchCount = parser.accepts("batch", "Batch count")
            .withRequiredArg().ofType(Integer.class).defaultsTo(2048);

        workDirOpt = parser.accepts("work-dir", "Directory used for storing temporary files")
            .withRequiredArg().ofType(File.class).defaultsTo(new File("temp"));
        outputDirOpt = parser.accepts("out-dir", "Directory for storing output files")
            .withRequiredArg().ofType(File.class).defaultsTo(new File("datastore-out"));

        verbose =
            parser.accepts("verbose", "Option to get all the paths and implementation specific blob ids");

        resetLoggingConfig =
            parser.accepts("reset-log-config", "Reset logging config for testing purposes only").withOptionalArg()
                .ofType(Boolean.class).defaultsTo(Boolean.TRUE);
        exportMetrics = parser.accepts("export-metrics",
            "type, URI to export the metrics and optional metadata all delimeted by semi-colon(;)").withRequiredArg();

        //Set of options which define action
        actionOpts = ImmutableSet.of(collectGarbage, consistencyCheck);
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
        return "The datastore command supports the following operations.";
    }

    @Override
    public int order() {
        return 50;
    }

    @Override
    public Set<String> operationNames() {
        return operationNames;
    }

    public boolean anyActionSelected() {
        for (OptionSpec spec : actionOpts) {
            if (options.has(spec)){
                return true;
            }
        }
        return false;
    }

    public File getWorkDir() throws IOException {
        File workDir = workDirOpt.value(options);
        FileUtils.forceMkdir(workDir);
        return workDir;
    }

    public File getOutDir() {
        return outputDirOpt.value(options);
    }

    public boolean collectGarbage() {
        return options.has(collectGarbage);
    }

    public boolean checkConsistency(){
        return options.has(consistencyCheck);
    }

    public boolean markOnly() {
        return collectGarbage.value(options);
    }

    public long getBlobGcMaxAgeInSecs() {
        return blobGcMaxAgeInSecs.value(options);
    }

    public int getBatchCount() {
        return batchCount.value(options);
    }

    public boolean isVerbose() {
        return options.has(verbose);
    }

    public boolean isResetLoggingConfig() {
        return resetLoggingConfig.value(options);
    }

    private static Set<String> collectionOperationNames(Set<OptionSpec> actionOpts) {
        Set<String> result = new HashSet<>();
        for (OptionSpec spec : actionOpts){
            result.addAll(spec.options());
        }
        return result;
    }

    public boolean exportMetrics() {
        return options.has(exportMetrics);
    }

    public String exportMetricsArgs() {
        return exportMetrics.value(options);
    }
}
