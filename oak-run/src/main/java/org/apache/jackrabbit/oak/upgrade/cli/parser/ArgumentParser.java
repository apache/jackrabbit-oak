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

import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nonnull;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public final class ArgumentParser {

    private static final OptionParser OP;

    public static final OptionSpec<Void> COPY_BINARIES;

    public static final OptionSpec<Void> MMAP;

    public static final OptionSpec<Void> LDAP;

    public static final OptionSpec<Void> FAIL_ON_ERROR;

    public static final OptionSpec<Void> EARLY_SHUTDOWN;

    public static final OptionSpec<String> DISABLE_INDEXES;

    public static final OptionSpec<Integer> CACHE_SIZE;

    public static final OptionSpec<Void> HELP;

    public static final OptionSpec<Void> VERSION;

    public static final OptionSpec<String> DST_USER;

    public static final OptionSpec<String> DST_PASSWORD;

    public static final OptionSpec<String> SRC_USER;

    public static final OptionSpec<String> SRC_PASSWORD;

    public static final OptionSpec<String> SRC_FBS;

    public static final OptionSpec<String> SRC_FDS;

    public static final OptionSpec<String> DST_FBS;

    public static final OptionSpec<String> DST_S3;

    public static final OptionSpec<String> DST_S3_CONFIG;

    public static final OptionSpec<String> NON_OPTION;

    public static final OptionSpec<String> COPY_VERSIONS;

    public static final OptionSpec<String> COPY_ORPHANED_VERSIONS;

    public static final OptionSpec<String> INCLUDE_PATHS;

    public static final OptionSpec<String> EXCLUDE_PATHS;

    public static final OptionSpec<String> MERGE_PATHS;

    private final OptionSet options;

    private final List<String> arguments;

    private MigrationOptions migrationOptions;

    private StoreArguments storeArguments;

    static {
        OP = new OptionParser();

        // flags
        COPY_BINARIES = OP.accepts("copy-binaries",
                "Copy binary content. Use this to disable use of existing DataStore in new repo");
        MMAP = OP.accepts("mmap", "Enable memory mapped file access for Segment Store");
        LDAP = OP.accepts("ldap", "Create rep:externalId properties for LDAP principals");
        FAIL_ON_ERROR = OP.accepts("fail-on-error", "Fail completely if nodes can't be read from the source repo");
        EARLY_SHUTDOWN = OP.accepts("early-shutdown",
                "Shutdown the source repository after nodes are copied and before the commit hooks are applied");
        DISABLE_INDEXES = OP.accepts("disable-indexes", "Comma separated list of index names that need to be disabled")
                .withRequiredArg().ofType(String.class);
        CACHE_SIZE = OP.accepts("cache", "Cache size in MB").withRequiredArg().ofType(Integer.class).defaultsTo(256);

        // versioning
        COPY_VERSIONS = OP
                .accepts("copy-versions",
                        "Copy the version storage. Parameters: { true | false | yyyy-mm-dd }. Defaults to true.")
                .withRequiredArg().ofType(String.class);
        COPY_ORPHANED_VERSIONS = OP
                .accepts("copy-orphaned-versions",
                        "Allows to skip copying orphaned versions. Parameters: { true | false | yyyy-mm-dd }. Defaults to true.")
                .withRequiredArg().ofType(String.class);
        INCLUDE_PATHS = OP.accepts("include-paths", "Comma-separated list of paths to include during copy.")
                .withRequiredArg().ofType(String.class);
        EXCLUDE_PATHS = OP.accepts("exclude-paths", "Comma-separated list of paths to exclude during copy.")
                .withRequiredArg().ofType(String.class);
        MERGE_PATHS = OP.accepts("merge-paths", "Comma-separated list of paths to merge during copy.")
                .withRequiredArg().ofType(String.class);

        // info
        HELP = OP.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        VERSION = OP.accepts("version", "Print the version of this tool");

        // source
        SRC_USER = OP.accepts("src-user", "Source rdb user").withRequiredArg().ofType(String.class);
        SRC_PASSWORD = OP.accepts("src-password", "Source rdb password").withRequiredArg().ofType(String.class);
        SRC_FBS = OP.accepts("src-fileblobstore", "Datastore directory to be used as a source FileBlobStore")
                .withRequiredArg().ofType(String.class);
        SRC_FDS = OP.accepts("src-datastore", "Datastore directory to be used as a source FileDataStore")
                .withRequiredArg().ofType(String.class);

        // destination
        DST_USER = OP.accepts("user", "Target rdb user").withRequiredArg().ofType(String.class);
        DST_PASSWORD = OP.accepts("password", "Target rdb password").withRequiredArg().ofType(String.class);
        DST_FBS = OP.accepts("fileblobstore", "Datastore directory to be used as a target FileBlobStore")
                .withRequiredArg().ofType(String.class);
        DST_S3 = OP.accepts("s3datastore", "Repository home to be used for the target S3").withRequiredArg()
                .ofType(String.class);
        DST_S3_CONFIG = OP.accepts("s3config", "Configuration file for the target S3DataStore").withRequiredArg()
                .ofType(String.class);

        NON_OPTION = OP.nonOptions(
                "[/path/to/oak/repository|/path/to/crx2/repository|mongodb://host:port|<Jdbc URI>] [/path/to/repository.xml] {/path/to/oak/repository|mongodb://host:port|<Jdbc URI>}");
    }

    private ArgumentParser(String[] args) {
        options = OP.parse(args);
        arguments = NON_OPTION.values(options);
    }

    boolean hasOption(OptionSpec<?> optionSpec) {
        return options.has(optionSpec);
    }

    String getOption(OptionSpec<String> optionSpec) {
        return optionSpec.value(options);
    }

    int getIntOption(OptionSpec<Integer> optionSpec) {
        return optionSpec.value(options);
    }

    public MigrationOptions getOptions() {
        if (migrationOptions == null) {
            migrationOptions = new MigrationOptions(this);
        }
        return migrationOptions;
    }

    public StoreArguments getStoreArguments() throws CliArgumentException, IOException {
        if (storeArguments == null) {
            storeArguments = new StoreArguments(this);
        }
        return storeArguments;
    }

    public static ArgumentParser parse(String args[]) throws IOException, CliArgumentException {
        final ArgumentParser argParser = new ArgumentParser(args);
        if (argParser.hasOption(HELP)) {
            OP.printHelpOn(System.out);
            return null;
        }
        if (argParser.hasOption(VERSION)) {
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
     * @return the version
     */
    @Nonnull
    private static String getVersion() {
        InputStream stream = ArgumentParser.class.getResourceAsStream(
                "/META-INF/maven/com.adobe.granite/crx2oak/pom.properties");
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
