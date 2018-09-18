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
package org.apache.jackrabbit.oak.console;


import java.util.Collections;
import java.util.List;


import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.codehaus.groovy.tools.shell.IO;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.codehaus.groovy.tools.shell.util.Preferences;

/**
 * A command line console.
 */
public class Console {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec quiet = parser.accepts("quiet", "be less chatty");
        OptionSpec shell = parser.accepts("shell", "run the shell after executing files");

        Options opts = new Options();
        OptionSet options = opts.parseAndConfigure(parser, args);

        int code = 0;
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
            List<String> nonOptions = opts.getCommonOpts().getNonOptions();
            List<String> scriptArgs = nonOptions.size() > 1 ?
                    nonOptions.subList(1, nonOptions.size()) : Collections.emptyList();
            IO io = new IO();

            if (options.has(quiet)) {
                io.setVerbosity(IO.Verbosity.QUIET);
            }

            if (!opts.getCommonOpts().isReadWrite()) {
                io.out.println("Repository connected in read-only mode. Use '--read-write' for write operations");
            }

            GroovyConsole console =
                    new GroovyConsole(ConsoleSession.create(fixture.getStore(), fixture.getWhiteboard()), new IO(), fixture);

            if (!scriptArgs.isEmpty()) {
                if (!options.has(shell)) {
                    Preferences.verbosity = IO.Verbosity.QUIET;
                }
                code = console.execute(scriptArgs);
            }

            if (scriptArgs.isEmpty() || options.has(shell)) {
                code = console.run();
            }
        }

        System.exit(code);
    }
}