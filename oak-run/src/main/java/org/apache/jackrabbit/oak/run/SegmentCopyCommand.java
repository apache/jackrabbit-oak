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

import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.aws.tool.AwsSegmentCopy;
import org.apache.jackrabbit.oak.segment.azure.tool.SegmentCopy;

import java.io.IOException;
import java.io.PrintWriter;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

class SegmentCopyCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<Integer> last = parser.accepts("last", "define the number of revisions to be copied (default: 1)")
                .withOptionalArg()
                .ofType(Integer.class);

        OptionSpec<Void> flat = parser.accepts("flat", "copy segments in flat hierarchy");

        OptionSpec<Void> append = parser.accepts("append", "skip existing segments");

        OptionSpec<Integer> maxSizeGb = parser.accepts("max-size-gb", "define maximum size of archives to be copied")
                .withOptionalArg()
                .ofType(Integer.class);

        OptionSet options = parser.parse(args);

        PrintWriter out = new PrintWriter(System.out, true);
        PrintWriter err = new PrintWriter(System.err, true);

        if (options.nonOptionArguments().size() != 2) {
            printUsage(parser, err);
        }

        String source = options.nonOptionArguments().get(0).toString();
        String destination = options.nonOptionArguments().get(1).toString();

        if (AwsSegmentCopy.canExecute(source, destination)) {
            AwsSegmentCopy.Builder builder = AwsSegmentCopy.builder()
                    .withSource(source)
                    .withDestination(destination)
                    .withOutWriter(out)
                    .withErrWriter(err);

            if (options.has(last)) {
                builder.withRevisionsCount(last.value(options) != null ? last.value(options) : 1);
            }

            System.exit(builder.build().run());
        } else {
            SegmentCopy.Builder builder = SegmentCopy.builder()
                    .withSource(source)
                    .withDestination(destination)
                    .withOutWriter(out)
                    .withErrWriter(err)
                    .withAppendMode(options.has(append));

            if (options.has(last)) {
                builder.withRevisionsCount(last.value(options) != null ? last.value(options) : 1);
            }

            if (options.has(maxSizeGb)) {
                builder.withMaxSizeGb(maxSizeGb.value(options));
                builder.withFlat(options.has(flat));
            }

            System.exit(builder.build().run());
        }
    }

    private void printUsage(OptionParser parser, PrintWriter err, String... messages) throws IOException {
        for (String message : messages) {
            err.println(message);
        }

        err.println("usage: segment-copy src dest [options] \n");
        err.println("       where src/dest are specified as PATH | cloud-prefix:URI");
        parser.printHelpOn(err);
        System.exit(1);
    }
}