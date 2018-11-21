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

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.tool.RecoverJournal;

class RecoverJournalCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
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

        if (parsed.valuesOf(dir).size() == 0) {
            System.err.println("Segment Store path not specified");
            System.exit(1);
        }

        if (parsed.valuesOf(dir).size() > 1) {
            System.err.println("Too many Segment Store paths specified");
            System.exit(1);
        }

        int code = RecoverJournal.builder()
            .withPath(dir.value(parsed))
            .withOut(System.out)
            .withErr(System.err)
            .build()
            .run();
        System.exit(code);
    }

}
