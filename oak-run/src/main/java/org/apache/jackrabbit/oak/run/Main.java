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

import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.Utils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Locale;

import static java.util.Arrays.copyOfRange;
import static org.apache.jackrabbit.oak.run.AvailableModes.MODES;

public final class Main {
    private Main() {
        // Prevent instantiation.
    }

    public static boolean commandCompleted = false;

    public static final String PROP_PRINT_STACK = "printStackTraceOnSignal";

    public static Thread shutdownHook = new Thread( () -> {
        if (!commandCompleted) {
            System.err.println("oak-run was interrupted by a signal and stopped");
            if (System.getProperty(PROP_PRINT_STACK) != null) {
                System.err.println("Dumping threads as requested");
                ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
                for(ThreadInfo threadInfo : threadMXBean.dumpAllThreads(true, true)) {
                    System.err.println(org.apache.jackrabbit.oak.run.Utils.formatThreadInfo(threadInfo));
                }
            }
         }
    });

    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        Utils.printProductInfo(
            args,
            Main.class.getResourceAsStream("/META-INF/maven/org.apache.jackrabbit/oak-run/pom.properties"));

        Command command = MODES.getCommand("help");

        if (args.length > 0) {
            command = MODES.getCommand(args[0].toLowerCase(Locale.ENGLISH));

            if (command == null) {
                command = MODES.getCommand("help");
            }

            args = copyOfRange(args, 1, args.length);
        }

        int exitcode = command.execute(args);
        commandCompleted = true;
        // when this point is reached, the shutdownHook is no longer required; there is a slight
        // chance that the signal is arriving now, but we just ignore this case.
        
        System.exit(exitcode);
    }
}
