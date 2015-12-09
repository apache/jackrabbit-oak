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
package org.apache.jackrabbit.oak.upgrade.cli;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.upgrade.cli.parser.CliArgumentException;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationCliArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class CliUtils {

    private static final Logger log = LoggerFactory.getLogger(OakUpgrade.class);

    public static MigrationCliArguments parseOrExit(OptionParser op, String... args) {
        try {
            OptionSet options = op.parse(args);
            if (options.has(OptionParserFactory.HELP) || options.nonOptionArguments().isEmpty()) {
                displayUsage();
                return null;
            }
            return new MigrationCliArguments(options);
        } catch (Exception e) {
            System.exit(getReturnCode(e));
            return null;
        }
    }

    public static void displayUsage() throws IOException {
        System.out.println(getUsage().replace("${command}", "java -jar oak-run-*-jr2.jar upgrade"));
    }

    public static String getUsage() throws IOException {
        InputStream is = CliUtils.class.getClassLoader().getResourceAsStream("upgrade_usage.txt");
        try {
            return IOUtils.toString(is);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    public static int getReturnCode(Exception e) {
        if (e.getMessage() != null) {
            System.err.println(e.getMessage());
        }
        if (e instanceof CliArgumentException) {
            return ((CliArgumentException) e).getExitCode();
        } else {
            e.printStackTrace(System.err);
            return 1;
        }
    }

    public static void handleSigInt(final Closer closer) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    closer.close();
                } catch (IOException e) {
                    log.error("Can't close", e);
                }
            }
        });
    }
}
