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
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.tool.DebugSegments;
import org.apache.jackrabbit.oak.segment.tool.DebugStore;
import org.apache.jackrabbit.oak.segment.tool.DebugTars;

class DebugCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> nonOptions = parser.nonOptions().ofType(String.class);
        OptionSet options = parser.parse(args);

        if (options.valuesOf(nonOptions).isEmpty()) {
            System.err.println("usage: debug <path> [id...]");
            System.exit(1);
        }

        System.exit(debug(options.valuesOf(nonOptions)));
    }

    private static int debug(List<String> args) {
        File file = new File(args.get(0));

        List<String> tars = new ArrayList<>();
        List<String> segs = new ArrayList<>();

        for (int i = 1; i < args.size(); i++) {
            if (args.get(i).endsWith(".tar")) {
                tars.add(args.get(i));
            } else {
                segs.add(args.get(i));
            }
        }

        int returnCode = 0;

        if (tars.size() > 0) {
            if (debugTars(file, tars) != 0) {
                returnCode = 1;
            }
        }

        if (segs.size() > 0) {
            if (debugSegments(file, segs) != 0) {
                returnCode = 1;
            }
        }

        if (tars.isEmpty() && segs.isEmpty()) {
            if (debugStore(file) != 0) {
                returnCode = 1;
            }
        }

        return returnCode;
    }

    private static int debugTars(File store, List<String> tars) {
        DebugTars.Builder builder = DebugTars.builder().withPath(store);

        for (String tar : tars) {
            builder.withTar(tar);
        }

        return builder.build().run();
    }

    private static int debugSegments(File store, List<String> segments) {
        DebugSegments.Builder builder = DebugSegments.builder().withPath(store);

        for (String segment : segments) {
            builder.withSegment(segment);
        }

        return builder.build().run();
    }

    private static int debugStore(File store) {
        return DebugStore.builder()
            .withPath(store)
            .build()
            .run();
    }

}