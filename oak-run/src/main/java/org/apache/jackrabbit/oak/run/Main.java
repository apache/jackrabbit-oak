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

import java.util.Locale;

import static java.util.Arrays.copyOfRange;
import static org.apache.jackrabbit.oak.run.AvailableModes.MODES;

public final class Main {
    private Main() {
        // Prevent instantiation.
    }

    public static void main(String[] args) throws Exception {
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

        command.execute(args);
    }
}
