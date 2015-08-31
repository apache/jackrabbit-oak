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
package org.apache.jackrabbit.oak.upgrade.cli.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nonnull;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public final class ArgumentParser {

    private final OptionSet options;

    private final List<String> arguments;

    private final MigrationOptions migrationOptions;

    private final StoreArguments storeArguments;

    private ArgumentParser(String[] args, OptionParser op) throws CliArgumentException, IOException {
        options = op.parse(args);
        arguments = op.nonOptions().values(options);
        migrationOptions = new MigrationOptions(this);
        storeArguments = new StoreArguments(this);
    }

    public boolean hasOption(String optionName) {
        return options.has(optionName);
    }

    public String getOption(String optionName) {
        return (String) options.valueOf(optionName);
    }

    public int getIntOption(String optionName) {
        return (Integer) options.valueOf(optionName);
    }
    
    public String[] getOptionList(String optionName) {
        return getOption(optionName).split(",");
    }

    public MigrationOptions getOptions() {
        return migrationOptions;
    }

    public StoreArguments getStoreArguments() {
        return storeArguments;
    }

    public static ArgumentParser parse(String args[], OptionParser op) throws IOException, CliArgumentException {
        final ArgumentParser argParser = new ArgumentParser(args, op);
        if (argParser.hasOption(OptionParserFactory.HELP)) {
            op.printHelpOn(System.out);
            return null;
        }
        if (argParser.hasOption(OptionParserFactory.VERSION)) {
            System.out.println("Crx2Oak version " + getVersion());
            return null;
        }
        return argParser;
    }

    List<String> getArguments() {
        return arguments;
    }

    /**
     * Returns the version of crx2oak bundle
     * 
     * @return the version
     */
    @Nonnull
    private static String getVersion() {
        InputStream stream = ArgumentParser.class
                .getResourceAsStream("/META-INF/maven/org.apache.jackrabbit/oak-run/pom.properties");
        if (stream != null) {
            try {
                try {
                    Properties properties = new Properties();
                    properties.load(stream);
                    return properties.getProperty("version");
                } finally {
                    stream.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return "unknown version";
    }
}
