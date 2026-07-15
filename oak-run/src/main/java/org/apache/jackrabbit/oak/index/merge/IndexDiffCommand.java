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
package org.apache.jackrabbit.oak.index.merge;

import static java.util.Arrays.asList;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.jackrabbit.oak.run.commons.Command;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class IndexDiffCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> customDirOption = parser
                .accepts("custom", "Path to index definition files (.json). " +
                        "List differences between out-of-the-box and customized indexes").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> mergeDirectoryOption = parser
                .accepts("merge", "Path to index definition files (.json). " +
                        "This is the existing indexes when merging").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> mergeAddFileOption = parser
                .accepts("merge-add", "Path to index definition file (.json) to merge. " +
                        "Adds the new out-of-the-box index to each index definition.").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> comparePathOption = parser
                .accepts("compare", "Path to index definition file or files (.json). " +
                        "Compare index1 and index2, or all indexes there to the indexBase file").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> index1Option = parser
                .accepts("index1", "Index 1").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> index2Option = parser
                .accepts("index2", "Index 2").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> indexBaseOption = parser
                .accepts("indexBase", "Index base file").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> extractFileOption = parser
                .accepts("extract", "File from where to extract (an) index definition(s) (.json). " +
                        "This will extract index1, or all indexes").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> targetDirectoryOption = parser
                .accepts("target", "Target directory where to store results.")
                .withOptionalArg()
                .defaultsTo("");

        OptionSpec<?> helpSpec = parser.acceptsAll(
                asList("h", "?", "help"), "show help").forHelp();

        OptionSet options = parser.parse(args);
        String customDir = customDirOption.value(options);
        String mergeDirectory = mergeDirectoryOption.value(options);
        String comparePath = comparePathOption.value(options);
        String extractFile = extractFileOption.value(options);
        String targetDirectory = targetDirectoryOption.value(options);
        if (options.has(helpSpec) || (customDir.isEmpty() &&
                mergeDirectory.isEmpty() &&
                comparePath.isEmpty() &&
                extractFile.isEmpty())) {
            parser.printHelpOn(System.out);
            return;
        }
        if (!customDir.isEmpty()) {
            System.out.println("Listing differences between out-of-the-box and customized indexes " +
                    "for directory \"" + customDir + "\"");
            System.out.println(IndexDiff.collectCustomizations(customDir));
        }
        if (!mergeDirectory.isEmpty()) {
            String mergeAdd = mergeAddFileOption.value(options);
            if (mergeAdd.isEmpty()) {
                parser.printHelpOn(System.out);
                return;
            }
            if (targetDirectory.isEmpty()) {
                System.out.println("Merging indexes " +
                        "for directory \"" +
                        mergeDirectory +
                        "\" with \"" + mergeAdd + "\"");
                System.out.println(IndexDiff.mergeIndexes(mergeDirectory, mergeAdd));
            } else {
                System.out.println("Merging index " +
                        "for \"" +
                        mergeDirectory +
                        "\" with \"" + mergeAdd + "\"");
                IndexDiff.mergeIndex(mergeDirectory, mergeAdd, targetDirectory);
            }
        }
        if (!comparePath.isEmpty()) {
            String index1 = index1Option.value(options);
            String index2 = index2Option.value(options);
            String indexBaseFile = indexBaseOption.value(options);
            if (!indexBaseFile.isEmpty()) {
                System.out.println("Comparing indexes in " + indexBaseFile + " against all indexes in " +
                        " directory \"" +
                        comparePath + "\"");
                System.out.println(IndexDiff.compareIndexesAgainstBase(comparePath, indexBaseFile));
            } else {
                if (index1.isEmpty() || index2.isEmpty()) {
                    parser.printHelpOn(System.out);
                    return;
                }
                if (Files.isRegularFile(Paths.get(comparePath))) {
                    System.out.println("Comparing indexes " + index1 + " and " + index2 +
                            " in file \"" +
                            comparePath + "\"");
                    System.out.println(IndexDiff.compareIndexesInFile(Paths.get(comparePath), index1, index2));
                } else {
                    System.out.println("Comparing indexes " + index1 + " and " + index2 +
                            " for directory \"" +
                            comparePath + "\"");
                    System.out.println(IndexDiff.compareIndexes(comparePath, index1, index2));
                }
            }
        }
        if (!extractFile.isEmpty()) {
            String index1 = index1Option.value(options);
            if (!index1.isEmpty()) {
                System.out.println("Extracting index " + index1 + " from \"" +
                        extractFile + "\"");
                System.out.println(IndexDiff.extract(extractFile, index1));
            } else if (!targetDirectory.isEmpty()) {
                System.out.println("Extracting indexes to \"" + targetDirectory + "\" from \"" +
                        extractFile + "\"");
                IndexDiff.extractAll(extractFile, targetDirectory);
            }
        }
    }

}
