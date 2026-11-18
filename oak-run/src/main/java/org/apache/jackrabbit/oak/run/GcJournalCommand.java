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

import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;

import java.io.File;
import java.io.PrintWriter;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.tool.GcJournalCheck;

/**
 * Parent command for gc.log (GC journal) tooling. Dispatches on a sub-command
 * so new actions can be added later.
 */
class GcJournalCommand implements Command {

    private static final String CHECK = "check";

    private static final String USAGE = String.join(System.lineSeparator(),
        "gc-journal <sub-command> <path>",
        "where sub-command is one of",
        "  check     report whether gc.log is well-formed, readable and up to date");

    @Override
    public void execute(String... args) throws Exception {
        if (args.length == 0) {
            System.err.println(USAGE);
            System.exit(1);
        }

        String subCommand = args[0];
        File path = parsePath(copyOfRange(args, 1, args.length));

        int code;
        if (CHECK.equals(subCommand)) {
            code = GcJournalCheck.builder()
                .withPath(path)
                .withOutWriter(new PrintWriter(System.out, true))
                .withErrWriter(new PrintWriter(System.err, true))
                .build()
                .run();
        } else {
            System.err.println("Unknown sub-command: " + subCommand);
            System.err.println(USAGE);
            System.exit(1);
            return;
        }

        System.exit(code);
    }

    private static File parsePath(String... args) throws Exception {
        OptionParser options = new OptionParser();
        OptionSpec<?> help = options.acceptsAll(asList("h", "help"), "Prints help and exits");
        OptionSpec<File> dir = options.nonOptions()
            .describedAs("path")
            .ofType(File.class);
        OptionSet parsed = options.parse(args);

        if (parsed.has(help)) {
            options.printHelpOn(System.out);
            System.exit(0);
        }

        if (parsed.valuesOf(dir).size() != 1) {
            System.err.println("Expected exactly one Segment Store path");
            System.exit(1);
        }

        return dir.value(parsed);
    }

}
