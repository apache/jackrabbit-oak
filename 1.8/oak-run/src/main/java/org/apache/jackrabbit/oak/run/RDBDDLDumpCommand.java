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

import java.util.Arrays;

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBHelper;
import org.apache.jackrabbit.oak.run.commons.Command;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class RDBDDLDumpCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Void> helpOption = parser.accepts("help", "show help").forHelp();
        OptionSpec<String> dbOption = parser.accepts("db", "Database type: one of " + RDBHelper.getSupportedDatabases())
                .withRequiredArg().ofType(String.class);
        OptionSpec<Integer> initialSchemaOption = parser.accepts("initial", "Initial DB schema version").withRequiredArg()
                .ofType(Integer.class);
        OptionSpec<Integer> upgradeSchemaOption = parser.accepts("upgrade", "DB schema version to upgrade to").withRequiredArg()
                .ofType(Integer.class);

        try {
            OptionSet options = parser.parse(args);

            if (options.has(helpOption)) {
                System.err.println("Options:");
                parser.printHelpOn(System.err);
            } else {
                String db = dbOption.value(options);
                Integer initial = initialSchemaOption.value(options);
                Integer upgrade = upgradeSchemaOption.value(options);
                RDBHelper.dump(db, initial, upgrade);
            }
            System.exit(0);
        } catch (joptsimple.OptionException ex) {
            System.err.println(ex.getLocalizedMessage());
            System.err.println(Arrays.toString(args));
            System.err.println();
            System.err.println("Options:");
            parser.printHelpOn(System.err);
            System.exit(2);
        }
    }
}
