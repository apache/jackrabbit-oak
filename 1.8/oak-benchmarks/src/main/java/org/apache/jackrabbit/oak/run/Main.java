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

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.Modes;
import org.apache.jackrabbit.oak.run.commons.Utils;

import static java.util.Arrays.copyOfRange;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

public final class Main {
    private static final Modes MODES = new Modes(ImmutableMap.<String, Command>of(
        "benchmark", new BenchmarkCommand(),
        "scalability", new ScalabilityCommand()
    ));

    private Main() {
        // Prevent instantiation.
    }

    public static void main(String[] args) throws Exception {
        Utils.printProductInfo(
            args,
            Main.class.getResourceAsStream("/META-INF/maven/org.apache.jackrabbit/oak-benchmarks/pom.properties")
        );

        Command c = MODES.getCommand("benchmark");
        if (args.length > 0) {
            c = MODES.getCommand(args[0]);

            if (c == null) {
                c = MODES.getCommand("benchmark");
            }

            args = copyOfRange(args, 1, args.length);
        }

        c.execute(args);
    }
}
