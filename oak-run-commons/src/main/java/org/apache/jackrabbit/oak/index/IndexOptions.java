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
package org.apache.jackrabbit.oak.index;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.Sets;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.run.cli.OptionsBean;
import org.apache.jackrabbit.oak.run.cli.OptionsBeanFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IndexOptions implements OptionsBean {

    public static final OptionsBeanFactory FACTORY = IndexOptions::new;

    private final OptionSpec<File> workDirOpt;
    private final OptionSpec<File> outputDirOpt;
    private final OptionSpec<File> indexImportDir;
    private final OptionSpec<File> preExtractedTextOpt;
    private final OptionSpec<File> indexDefinitionsOpt;
    private final OptionSpec<Void> stats;
    private final OptionSpec<Void> definitions;
    private final OptionSpec<Void> dumpIndex;
    private final OptionSpec<Void> reindex;
    private final OptionSpec<Void> ignoreMissingTikaDep;
    private final OptionSpec<Void> asyncIndex;
    private final OptionSpec<Void> importIndex;
    private final OptionSpec<Void> docTraversal;
    private final OptionSpec<Void> enableCowCor;
    private final OptionSpec<Void> buildFlatFileStoreSeparately;
    private final OptionSpec<Integer> consistencyCheck;
    private final OptionSpec<Long> asyncDelay;
    protected OptionSet options;
    protected final Set<OptionSpec> actionOpts;
    private final OptionSpec<String> indexPaths;
    private final OptionSpec<String> checkpoint;
    private final OptionSpec<String> asyncIndexLanes;
    private final Set<String> operationNames;
    private final OptionSpec<File> existingDataDumpDirOpt;



    public IndexOptions(OptionParser parser){
        workDirOpt = parser.accepts("index-temp-dir", "Directory used for storing temporary files")
                .withRequiredArg().ofType(File.class).defaultsTo(new File("temp"));
        outputDirOpt = parser.accepts("index-out-dir", "Directory used for output files")
                .withRequiredArg().ofType(File.class).defaultsTo(new File("indexing-result"));
        preExtractedTextOpt = parser.accepts("pre-extracted-text-dir", "Directory storing pre extracted text")
                .withRequiredArg().ofType(File.class);
        indexDefinitionsOpt = parser.accepts("index-definitions-file", "index definition file which " +
                "include new index definitions or changes to existing index definitions")
                .withRequiredArg().ofType(File.class);

        stats = parser.accepts("index-info", "Collects and dumps various statistics related to the indexes");
        definitions = parser.accepts("index-definitions", "Collects and dumps index definitions");
        indexPaths = parser.accepts("index-paths", "Comma separated list of index paths for which the " +
                "selected operations need to be performed")
                .withRequiredArg().ofType(String.class).withValuesSeparatedBy(",");

        checkpoint = parser.accepts("checkpoint", "The checkpoint up to which the index is updated, when " +
                "indexing in read only mode. For testing purpose, it can be set to 'head' to indicate that the head " +
                "state should be used.")
                .withRequiredArg().ofType(String.class);

        consistencyCheck = parser.accepts("index-consistency-check", "Performs consistency check " +
                "for indexes as specified by --index-paths (if this not set, all indexes are checked). Currently " +
                "only Lucene indexes are supported. Possible values 1 - Basic check, 2 - Full check (slower)")
                .withOptionalArg().ofType(Integer.class).defaultsTo(1);

        asyncDelay = parser.accepts("async-delay", "Delay (in seconds) between the execution of async cycles for a given lane")
                .withOptionalArg().ofType(Long.class).defaultsTo(5L);

        dumpIndex = parser.accepts("index-dump", "Dumps index content");
        reindex = parser.accepts("reindex", "Reindex the indexes specified by --index-paths or --index-definitions-file");
        ignoreMissingTikaDep = parser.accepts("ignore-missing-tika-dep", "Ignore when there are missing tika dependencies and continue to run");
        asyncIndex = parser.accepts("async-index", "Runs async index cycle");

        asyncIndexLanes = parser.accepts("async-index-lanes", "Comma separated list of async index lanes for which the " +
                "async index cycles would run")
                .withRequiredArg().ofType(String.class).withValuesSeparatedBy(",");

        importIndex = parser.accepts("index-import", "Imports index");
        docTraversal = parser.accepts("doc-traversal-mode", "Use Document traversal mode for reindex in " +
                "DocumentNodeStore setups. This may provide better performance in some cases (experimental)");
        enableCowCor = parser.accepts("enable-cow-cor", "Enables COW/COR during async indexing using oak-run");
        buildFlatFileStoreSeparately = parser.accepts("build-flatfilestore-separately", "Builds FlatFileStore as a separate step and then uses it as part of the doc-traversal-mode for reindexing");

        indexImportDir = parser.accepts("index-import-dir", "Directory containing index files. This " +
                "is required when --index-import operation is selected")
                .requiredIf(importIndex)
                .withRequiredArg().ofType(File.class);

        //Set of options which define action
        actionOpts = Set.of(stats, definitions, consistencyCheck, dumpIndex, reindex, importIndex);
        operationNames = collectionOperationNames(actionOpts);
        existingDataDumpDirOpt = parser.accepts("existing-data-dump-dir", "Directory containing document store dumps" +
                " from previous incomplete run")
                .withRequiredArg().ofType(File.class);
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
        return "The index command supports the following operations. Most operations are read only.\n" + 
                "BloStore related options must be provided, as operations access the binaries stored there.\n" +
                "If no explicit operation is selected, --index-info and --index-definitions operation are performed.\n" +
                "Use --index-paths to restrict the set of indexes on which the operation needs to be run.";
    }

    @Override
    public int order() {
        return 50;
    }

    @Override
    public Set<String> operationNames() {
        return operationNames;
    }

    public File getWorkDir() throws IOException {
        File workDir = workDirOpt.value(options);
        FileUtils.forceMkdir(workDir);
        return workDir;
    }

    public File getExistingDataDumpDir() {
        File dataDumpDir = existingDataDumpDirOpt.value(options);
        return dataDumpDir != null && dataDumpDir.exists() ? dataDumpDir : null;
    }

    public File getOutDir() {
        return outputDirOpt.value(options);
    }

    public File getPreExtractedTextDir() {
        return preExtractedTextOpt.value(options);
    }

    public File getIndexDefinitionsFile() {
        return indexDefinitionsOpt.value(options);
    }

    public File getIndexImportDir() {
        return indexImportDir.value(options);
    }

    public boolean dumpStats(){
        return options.has(stats) || !anyActionSelected();
    }

    public boolean dumpDefinitions(){
        return options.has(definitions) || !anyActionSelected();
    }

    public boolean dumpIndex() {
        return options.has(dumpIndex);
    }

    public boolean checkConsistency(){
        return options.has(consistencyCheck);
    }

    public int consistencyCheckLevel(){
        return consistencyCheck.value(options);
    }

    public long aysncDelay() {
        return  asyncDelay.value(options);
    }

    public boolean isReindex() {
        return options.has(reindex);
    }

    public boolean isIgnoreMissingTikaDep() {
        return options.has(ignoreMissingTikaDep);
    }

    public boolean isAsyncIndex() {
        return options.has(asyncIndex);
    }

    public boolean isImportIndex() {
        return options.has(importIndex);
    }

    public boolean isDocTraversalMode() {
        return  options.has(docTraversal);
    }

    public boolean isCowCorEnabled() {
        return options.has(enableCowCor);
    }

    public boolean buildFlatFileStoreSeparately() {
        return options.has(buildFlatFileStoreSeparately);
    }

    public String getCheckpoint(){
        return checkpoint.value(options);
    }

    public List<String> getIndexPaths(){
        return options.has(indexPaths) ? trim(indexPaths.values(options)) : Collections.emptyList();
    }

    public List<String> getAsyncLanes(){
        return options.has(asyncIndexLanes) ? trim(asyncIndexLanes.values(options)) : Collections.emptyList();
    }

    private boolean anyActionSelected(){
        for (OptionSpec spec : actionOpts){
            if (options.has(spec)){
                return true;
            }
        }
        return false;
    }

    private static List<String> trim(List<String> values) {
        Set<String> paths = new HashSet<>();
        for (String v : values) {
            v = Strings.emptyToNull(v);
            if (v != null) {
                paths.add(v.trim());
            }
        }
        return new ArrayList<>(paths);
    }

    private static Set<String> collectionOperationNames(Set<OptionSpec> actionOpts){
        Set<String> result = new HashSet<>();
        for (OptionSpec spec : actionOpts){
            result.addAll(spec.options());
        }
        return result;
    }
}
