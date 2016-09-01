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
import java.io.FileOutputStream;
import java.util.Calendar;
import java.util.Date;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

class GraphCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> directoryArg = parser.nonOptions(
                "Path to segment store (required)").ofType(File.class);
        OptionSpec<File> outFileArg = parser.accepts(
                "output", "Output file").withRequiredArg().ofType(File.class)
                .defaultsTo(new File("segments.gdf"));
        OptionSpec<Long> epochArg = parser.accepts(
                "epoch", "Epoch of the segment time stamps (derived from journal.log if not given)")
                .withRequiredArg().ofType(Long.class);
        OptionSpec<Void> gcGraphArg = parser.accepts(
                "gc", "Write the gc generation graph instead of the full graph");
        OptionSpec<String> regExpArg = parser.accepts(
                "pattern", "Regular exception specifying which nodes to include (optional). " +
                        "Ignore when --gc is specified.")
                .withRequiredArg().ofType(String.class);
        OptionSpec segment = parser.accepts("segment", "Use oak-segment instead of oak-segment-tar");

        OptionSet options = parser.parse(args);

        File directory = directoryArg.value(options);
        if (directory == null) {
            System.err.println("Dump the segment graph to a file. Usage: graph [File] <options>");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        String regExp = regExpArg.value(options);

        File outFile = outFileArg.value(options);
        Date epoch;
        if (options.has(epochArg)) {
            epoch = new Date(epochArg.value(options));
        } else {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(new File(directory, "journal.log").lastModified());
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
            epoch = c.getTime();
        }

        if (outFile.exists()) {
            outFile.delete();
        }

        System.out.println("Setting epoch to " + epoch);
        System.out.println("Writing graph to " + outFile.getAbsolutePath());
        FileOutputStream out = new FileOutputStream(outFile);

        boolean gcGraph = options.has(gcGraphArg);

        if (options.has(segment)) {
            SegmentUtils.graph(directory, gcGraph, epoch, regExp, out);
        } else {
            SegmentTarUtils.graph(directory, gcGraph, epoch, regExp, out);
        }
    }

}
