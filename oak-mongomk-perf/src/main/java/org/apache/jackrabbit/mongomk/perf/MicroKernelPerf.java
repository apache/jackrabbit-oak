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
package org.apache.jackrabbit.mongomk.perf;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.PropertyConfigurator;

/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class MicroKernelPerf {
    private static Config config;
    private static boolean masterMode;
    private static boolean prepareEnvironment;

    public static void main(String[] args) throws Exception {
        configLog4J(null);
        readConfig();

        evalCommandLineOptions(args);

        if (prepareEnvironment) {
            PrepareEnvironment prepare = new PrepareEnvironment(config);
            prepare.start();
        }

        if (masterMode) {
            MicroKernelPerfMaster master = new MicroKernelPerfMaster(config);
            master.start();
        } else {
            MicroKernelPerfClient client = new MicroKernelPerfClient(config);
            client.start();
        }
    }

    private static void configLog4J(String path) throws Exception {
        InputStream is = null;

        if (path == null) {
            is = MicroKernelPerfClient.class.getResourceAsStream("/log4j.cfg");
        } else {
            is = new FileInputStream(path);
        }

        Properties properties = new Properties();
        properties.load(is);
        is.close();

        PropertyConfigurator.configure(properties);
    }

    @SuppressWarnings("static-access")
    private static void evalCommandLineOptions(String[] args) throws Exception {
        Option log4jOption = OptionBuilder.withLongOpt("log4j-path").hasArg().withArgName("path")
                .withDescription("path to a log4j config file").create("log4j");
        Option masterOption = OptionBuilder.withLongOpt("master-mode").withDescription("starts this in master mode")
                .create("master");
        Option prepareOption = OptionBuilder.withLongOpt("prepare-environment")
                .withDescription("resets the environment before executing").create("prep");

        Options options = new Options();
        options.addOption(log4jOption);
        options.addOption(masterOption);
        options.addOption(prepareOption);

        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(options, args);

        if (line.hasOption(log4jOption.getOpt())) {
            configLog4J(line.getOptionValue(log4jOption.getOpt()));
        }
        if (line.hasOption(masterOption.getOpt())) {
            masterMode = true;
        }
        if (line.hasOption(prepareOption.getOpt())) {
            prepareEnvironment = true;
        }
    }

    private static void readConfig() throws Exception {
        InputStream is = MicroKernelPerfClient.class.getResourceAsStream("/config.cfg");

        Properties properties = new Properties();
        properties.load(is);

        is.close();

        config = new Config(properties);
    }
}
