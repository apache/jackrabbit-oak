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
import java.util.List;
import java.util.Set;

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
    private final OptionSpec<Boolean> consistencyCheck;
    private final OptionSpec<Void> refOp;
    private final OptionSpec<Void> idOp;
    private final OptionSpec<Boolean> checkConsistencyAfterGC;
    private final OptionSpec<Integer> batchCount;
    private final OptionSpec<Void> metadataOp;
    private OptionSet options;
    private final Set<OptionSpec> actionOpts;
    private final Set<String> operationNames;
    private final OptionSpec<Long> blobGcMaxAgeInSecs;
    private final OptionSpec<Void> verbose;
    private final OptionSpec<String> verboseRootPath;
    private final OptionSpec<String> verbosePathInclusionRegex;
    private final OptionSpec<Void> useDirListing;
    private final OptionSpec<Boolean> resetLoggingConfig;
    private OptionSpec<String> exportMetrics;
    private static final String DELIM = ",";
    private OptionSpec<Boolean> sweepIfRefsPastRetention;

    public DataStoreOptions(OptionParser parser) {
        collectGarbage = parser.accepts("collect-garbage",
            "Performs DataStore Garbage Collection on the repository/datastore defined. An option boolean specifying "
                + "'markOnly' required if only mark phase of garbage collection is to be executed")
            .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);

        checkConsistencyAfterGC = parser.accepts("check-consistency-gc",
            "Performs a consistency check immediately after DSGC")
            .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);

        sweepIfRefsPastRetention = parser.accepts("sweep-only-refs-past-retention",
            "Only allows sweep if all references available older than retention time (Default false)")
            .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);

        consistencyCheck =
            parser.accepts("check-consistency", "Performs a consistency check on the repository/datastore defined. An optional boolean specifying "
                + "'markOnly' required if only collecting references")
            .withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);

        refOp = parser.accepts("dump-ref", "Gets a dump of Blob References");

        idOp = parser.accepts("dump-id", "Gets a dump of Blob Ids");

        metadataOp = parser.accepts("get-metadata",
            "Gets the metadata available in the DataStore in the format `repositoryId|referencesTime|* (if local) "
                + "(earliest time of references file if available)` in the DataStore repository/datastore defined");

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

        // Option NOT available for garbage collection operation - we throw an
        // exception if both --collect-garbage and
        // --verboseRootPath are provided in the command.
        verboseRootPath = parser.accepts("verboseRootPath",
                "Root path to output backend formatted ids/paths").availableUnless(collectGarbage).availableIf(verbose)
                .withRequiredArg().withValuesSeparatedBy(DELIM).ofType(String.class);

        verbosePathInclusionRegex = parser.accepts("verbosePathInclusionRegex", "Regex to provide an inclusion list for " +
                "nodes that will be scanned under the path provided with the option --verboseRootPath").availableIf(verboseRootPath).
                withRequiredArg().withValuesSeparatedBy(DELIM).ofType(String.class);

        useDirListing = parser.accepts("useDirListing", "Use dirListing property for efficient reading of Lucene index files");

        resetLoggingConfig =
            parser.accepts("reset-log-config", "Reset logging config for testing purposes only").withOptionalArg()
                .ofType(Boolean.class).defaultsTo(Boolean.TRUE);
        exportMetrics = parser.accepts("export-metrics",
            "type, URI to export the metrics and optional metadata all delimeted by semi-colon(;)").withRequiredArg();

        //Set of options which define action
        actionOpts = Set.of(collectGarbage, consistencyCheck, idOp, refOp, metadataOp);
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

    public boolean dumpRefs() {
        return options.has(refOp);
    }

    public boolean dumpIds() {
        return options.has(idOp);
    }

    public boolean getMetadata(){
        return options.has(metadataOp);
    }

    public boolean checkConsistencyAfterGC() {
        return options.has(checkConsistencyAfterGC) && checkConsistencyAfterGC.value(options) ;
    }

    public boolean markOnly() {
        return collectGarbage.value(options);
    }

    public boolean consistencyCheckMarkOnly() {
        return consistencyCheck.value(options);
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

    public boolean hasVerboseRootPaths() {
        return options.has(verboseRootPath);
    }

    public boolean hasVerboseInclusionRegex() {
        return options.has(verbosePathInclusionRegex);
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

    public List<String> getVerboseRootPaths() {
        return options.valuesOf(verboseRootPath);
    }

    public List<String> getVerboseInclusionRegex() {
        return options.valuesOf(verbosePathInclusionRegex);
    }

    public boolean isUseDirListing() {
        return options.has(useDirListing);
    }

    public boolean sweepIfRefsPastRetention() {
        return options.has(sweepIfRefsPastRetention) && sweepIfRefsPastRetention.value(options) ;
    }
}
