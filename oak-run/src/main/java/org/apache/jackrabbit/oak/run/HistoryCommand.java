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

import static org.apache.jackrabbit.oak.segment.FileStoreHelper.isValidFileStoreOrFail;

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;

class HistoryCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> directoryArg = parser.nonOptions(
                "Path to segment store (required)").ofType(File.class);
        OptionSpec<String> journalArg = parser.accepts(
                "journal", "journal file").withRequiredArg().ofType(String.class)
                .defaultsTo("journal.log");
        OptionSpec<String> pathArg = parser.accepts(
                "path", "Path for which to trace the history").withRequiredArg().ofType(String.class)
                .defaultsTo("/");
        OptionSpec<Integer> depthArg = parser.accepts(
                "depth", "Depth up to which to dump node states").withRequiredArg().ofType(Integer.class)
                .defaultsTo(0);
        OptionSet options = parser.parse(args);

        File directory = directoryArg.value(options);
        if (directory == null) {
            System.err.println("Trace the history of a path. Usage: history [File] <options>");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        String path = pathArg.value(options);
        int depth = depthArg.value(options);
        String journalName = journalArg.value(options);
        File journal = new File(isValidFileStoreOrFail(directory), journalName);

        SegmentTarUtils.history(directory, journal, path, depth);
    }

}