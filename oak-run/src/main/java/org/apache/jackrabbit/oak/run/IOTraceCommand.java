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
 *
 */

package org.apache.jackrabbit.oak.run;

import static java.lang.String.format;
import static org.apache.jackrabbit.oak.segment.FileStoreHelper.isValidFileStoreOrFail;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import javax.annotation.Nonnull;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.tool.iotrace.IOTracer;
import org.apache.jackrabbit.oak.segment.tool.iotrace.Traces;

/**
 *  Command line utility for collection {@link IOTracer io traces}
 *
    <pre>
    usage: iotrace path/to/segmentstore <options>
    Option (* = required)      Description
    ---------------------      -----------
    --depth <Integer>          Maximal depth of the traversal (default: 5)
    --mmap <Boolean>           use memory mapping for the file store (default: true)
    --output <File>            output file where the IO trace is written to (default: iotrace.csv)
    --path <String>            starting path for the traversal (default: /root)
    --segment-cache <Integer>  size of the segment cache in MB (default: 256)
    --trace <Traces> (*)       type of the traversal. Either of [DEPTH, BREADTH]
    </pre>
 */
class IOTraceCommand implements Command {
    public static final String NAME = "iotrace";

    @Override
    public void execute(String... args) throws Exception {
        OptionParser optionParser = new OptionParser();
        ArgumentAcceptingOptionSpec<Traces> traceOption = optionParser
                .accepts("trace", "type of the traversal. Either of " + Arrays.toString(Traces.values()))
                .withRequiredArg()
                .ofType(Traces.class)
                .required();

        ArgumentAcceptingOptionSpec<File> outputOption = optionParser
                .accepts("output", "output file where the IO trace is written to")
                .withRequiredArg()
                .ofType(File.class)
                .defaultsTo(new File("iotrace.csv"));

        ArgumentAcceptingOptionSpec<Boolean> mmapOption = optionParser
                .accepts("mmap", "use memory mapping for the file store")
                .withRequiredArg()
                .ofType(Boolean.class)
                .defaultsTo(true);

        ArgumentAcceptingOptionSpec<Integer> segmentCacheOption = optionParser
                .accepts("segment-cache", "size of the segment cache in MB")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(256);

        ArgumentAcceptingOptionSpec<Integer> depthOption = optionParser
                .accepts("depth", "Maximal depth of the traversal")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(5);

        ArgumentAcceptingOptionSpec<String> pathOption = optionParser
                .accepts("path", "starting path for the traversal")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("/root");

        try {
            OptionSet options = optionParser.parse(args);

            if (options.nonOptionArguments().size() != 1) {
                printUsage(optionParser, System.err);
                System.exit(1);
            }

            File segmentStore = isValidFileStoreOrFail(new File(options.nonOptionArguments().get(0).toString()));
            Traces trace = traceOption.value(options);
            File output = outputOption.value(options);
            Boolean mmap = mmapOption.value(options);
            Integer segmentCache = segmentCacheOption.value(options);
            Integer depth = depthOption.value(options);
            String path = pathOption.value(options);

            System.out.println(
                    format("%s first traversing %d levels of %s starting at %s", trace, depth, segmentStore, path));
            System.out.println(
                    format("mmap=%b, segment cache=%d", mmap, segmentCache));
            System.out.println(
                    format("Writing trace to %s", output));

            trace.collectIOTrace(segmentStore, mmap, segmentCache, path, depth, output);
        } catch (OptionException e) {
            printUsage(optionParser, System.err, e.getMessage());
            System.exit(1);
        }
    }

    private static void printUsage(
            @Nonnull OptionParser parser,
            @Nonnull PrintStream err,
            @Nonnull String... messages)
    throws IOException {
        for (String message : messages) {
            err.println(message);
        }

        err.println("usage: " + NAME + " path/to/segmentstore <options>");
        parser.printHelpOn(err);
    }
}
