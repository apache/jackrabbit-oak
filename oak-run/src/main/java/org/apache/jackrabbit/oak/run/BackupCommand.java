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
        OptionSpec segment = parser.accepts("segment", "Use oak-segment instead of oak-segment-tar");
        OptionSet options = parser.parse(args);

        if (options.nonOptionArguments().size() < 2) {
            System.err.println("This command requires a source and a target folder");
            System.exit(1);
        }

        File source = new File(options.nonOptionArguments().get(0).toString());
        File target = new File(options.nonOptionArguments().get(1).toString());

        if (options.has(segment)) {
            SegmentUtils.backup(source, target);
        } else {
            SegmentTarUtils.backup(source, target);

        }
    }

}
