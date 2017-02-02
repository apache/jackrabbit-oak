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

import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.isValidFileStoreOrFail;

import java.io.File;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

class CheckCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> path = parser.accepts(
                "path", "path to the segment store (required)")
                .withRequiredArg().ofType(String.class);
        ArgumentAcceptingOptionSpec<String> journal = parser.accepts(
                "journal", "journal file")
                .withRequiredArg().ofType(String.class).defaultsTo("journal.log");
        OptionSpec deep = parser.accepts(
                "deep", "enable deep consistency checking. ");
        ArgumentAcceptingOptionSpec<Long> notify = parser.accepts(
                "notify", "number of seconds between progress notifications")
                .withRequiredArg().ofType(Long.class).defaultsTo(Long.MAX_VALUE);
        ArgumentAcceptingOptionSpec<Long> bin = parser.accepts(
                "bin", "read the n first bytes from binary properties. -1 for all bytes.")
                .withOptionalArg().ofType(Long.class).defaultsTo(0L);
        OptionSpec segment = parser.accepts("segment", "Use oak-segment instead of oak-segment-tar");

        OptionSet options = parser.parse(args);

        if (!options.has(path)) {
            System.err.println("usage: check <options>");
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        File dir = isValidFileStoreOrFail(new File(path.value(options)));
        String journalFileName = journal.value(options);
        boolean fullTraversal = options.has(deep);
        long debugLevel = notify.value(options);
        long binLen = bin.value(options);

        if (options.has(segment)) {
            SegmentUtils.check(dir, journalFileName, fullTraversal, debugLevel, binLen);
        } else {
            SegmentTarUtils.check(dir, journalFileName, fullTraversal, debugLevel, binLen);
        }
    }

}
