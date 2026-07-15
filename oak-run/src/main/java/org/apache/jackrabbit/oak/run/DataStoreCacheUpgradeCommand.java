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

import java.io.File;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.plugins.blob.DataStoreCacheUpgradeUtils;

import static java.util.Arrays.asList;

/**
 * Command to upgrade JR2 DataStore cache.
 */
public class DataStoreCacheUpgradeCommand implements Command {
    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        try {
            OptionSpec<File> homeDirOption = parser.accepts("homeDir",
                "Home directory of the datastore where the pending uploads is serialized")
                .withRequiredArg().ofType(File.class).required();
            OptionSpec<File> pathOption =
                parser.accepts("path", "Parent directory of the datastore").withRequiredArg()
                    .ofType(File.class).required();
            OptionSpec<Boolean> moveCacheOption = parser
                .accepts("moveCache", "Move DataStore download cache")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
            OptionSpec<Boolean> delPendingUploadsMapFileOption = parser
                .accepts("deleteMapFile", "Delete pending uploads file post upgrade")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
            OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();

            OptionSet options = null;
            try {
                options = parser.parse(args);
            } catch (Exception e) {
                System.err.println(e);
                parser.printHelpOn(System.err);
                return;
            }

            if (options.has(help)) {
                parser.printHelpOn(System.out);
                return;
            }

            File homeDir = options.valueOf(homeDirOption);
            File path = options.valueOf(pathOption);
            boolean moveCache = options.valueOf(moveCacheOption);
            boolean delPendingUploadsMapFile = options.valueOf(delPendingUploadsMapFileOption);

            System.out.println("homeDir " + homeDir);
            System.out.println("path " + path);
            System.out.println("moveCache " + moveCache);
            System.out.println("delPendingUploadsMapFile " + delPendingUploadsMapFile);
            DataStoreCacheUpgradeUtils.upgrade(homeDir, path, moveCache, delPendingUploadsMapFile);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error upgrading cache");
        }
    }
}
