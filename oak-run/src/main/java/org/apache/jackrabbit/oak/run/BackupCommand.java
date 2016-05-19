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

class BackupCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<File> folders = parser
                .nonOptions("source and target folders")
                .ofType(File.class);

        OptionSpec<Boolean> segmentTar = parser
                .accepts("segment-tar", "use new segment store implementation")
                .withOptionalArg()
                .ofType(Boolean.class)
                .defaultsTo(false);

        OptionSet options = parser.parse(args);

        if (folders.values(options).size() < 2) {
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        File source = folders.values(options).get(0);
        File target = folders.values(options).get(1);

        if (segmentTar.value(options) == Boolean.TRUE) {
            SegmentTarUtils.backup(source, target);
        } else {
            SegmentUtils.backup(source, target);
        }
    }

}
