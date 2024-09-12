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

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.tool.Utils.newBasicReadOnlyBlobStore;

import java.io.File;

import com.google.common.io.Files;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureStorageCredentialManagerV8;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.tool.Diff;
import org.apache.jackrabbit.oak.segment.tool.Revisions;

class FileStoreDiffCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        OptionSpec<String> pathOrURI0 = parser.nonOptions("Path/URI to segment store (required)").ofType(String.class);
        OptionSpec<File> outO = parser.accepts("output", "Output file").withRequiredArg().ofType(File.class).defaultsTo(defaultOutFile());
        OptionSpec<?> listOnlyO = parser.accepts("list", "Lists available revisions");
        OptionSpec<String> intervalO = parser.accepts("diff", "Revision diff interval. Ex '--diff=R0..R1'. 'HEAD' can be used to reference the latest head revision, ie. '--diff=R0..HEAD'").withRequiredArg().ofType(String.class);
        OptionSpec<?> incrementalO = parser.accepts("incremental", "Runs diffs between each subsequent revisions in the provided interval");
        OptionSpec<String> pathO = parser.accepts("path", "Filter diff by given path").withRequiredArg().ofType(String.class).defaultsTo("/");
        OptionSpec<?> ignoreSNFEsO = parser.accepts("ignore-snfes", "Ignores SegmentNotFoundExceptions and continues running the diff (experimental)");
        OptionSet options = parser.parse(args);

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        String pathOrURI = pathOrURI0.value(options);

        if (pathOrURI == null) {
            parser.printHelpOn(System.out);
            System.exit(1);
        }

        File out = outO.value(options);

        boolean listOnly = options.has(listOnlyO);
        String interval = intervalO.value(options);
        boolean incremental = options.has(incrementalO);
        String path = pathO.value(options);
        boolean ignoreSNFEs = options.has(ignoreSNFEsO);

        int statusCode;
        if (listOnly) {
            if (pathOrURI.startsWith("az:")) {
                statusCode = Revisions.builder()
                        .withPath(pathOrURI)
                        .withOutput(out)
                        .build()
                        .run(ToolUtils::readRevisions);
            } else {
                statusCode = Revisions.builder()
                    .withPath(pathOrURI)
                    .withOutput(out)
                    .build()
                    .run(org.apache.jackrabbit.oak.segment.tool.Utils::readRevisions);
            }
        } else {
            if (pathOrURI.startsWith("az:")) {
                try (AzureStorageCredentialManagerV8 azureStorageCredentialManagerV8 = new AzureStorageCredentialManagerV8()) {
                    SegmentNodeStorePersistence azurePersistence = ToolUtils.newSegmentNodeStorePersistence(ToolUtils.SegmentStoreType.AZURE, pathOrURI, azureStorageCredentialManagerV8);
                    ReadOnlyFileStore store = fileStoreBuilder(Files.createTempDir()).withCustomPersistence(azurePersistence).withBlobStore(newBasicReadOnlyBlobStore()).buildReadOnly();
                    statusCode = Diff.builder()
                            .withPath(pathOrURI)
                            .withReadOnlyFileStore(store)
                            .withOutput(out)
                            .withInterval(interval)
                            .withIncremental(incremental)
                            .withFilter(path)
                            .withIgnoreMissingSegments(ignoreSNFEs)
                            .withRevisionsProcessor(ToolUtils::readRevisions)
                            .build()
                            .run();
                }
            } else {
                ReadOnlyFileStore store = fileStoreBuilder(new File(pathOrURI)).withBlobStore(newBasicReadOnlyBlobStore()).buildReadOnly();
                statusCode = Diff.builder()
                    .withPath(pathOrURI)
                    .withReadOnlyFileStore(store)
                    .withOutput(out)
                    .withInterval(interval)
                    .withIncremental(incremental)
                    .withFilter(path)
                    .withIgnoreMissingSegments(ignoreSNFEs)
                    .withRevisionsProcessor(org.apache.jackrabbit.oak.segment.tool.Utils::readRevisions)
                    .build()
                    .run();
            }
        }
        System.exit(statusCode);
    }

    private File defaultOutFile() {
        return new File("diff_" + System.currentTimeMillis() + ".log");
    }

}