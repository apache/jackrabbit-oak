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

import joptsimple.OptionParser;

public class OptionParserFactory {

    public static final String COPY_BINARIES = "copy-binaries";

    public static final String MMAP = "mmap";

    public static final String FAIL_ON_ERROR = "fail-on-error";

    public static final String EARLY_SHUTDOWN = "early-shutdown";

    public static final String CACHE_SIZE = "cache";

    public static final String HELP = "help";

    public static final String DST_USER = "user";

    public static final String DST_PASSWORD = "password";

    public static final String SRC_USER = "src-user";

    public static final String SRC_PASSWORD = "src-password";

    public static final String SRC_FBS = "src-fileblobstore";

    public static final String SRC_FDS = "src-datastore";

    public static final String SRC_S3 = "src-s3datastore";

    public static final String SRC_S3_CONFIG = "src-s3config";

    public static final String DST_FDS = "datastore";

    public static final String DST_FBS = "fileblobstore";

    public static final String DST_S3 = "s3datastore";

    public static final String DST_S3_CONFIG = "s3config";

    public static final String COPY_VERSIONS = "copy-versions";

    public static final String COPY_ORPHANED_VERSIONS = "copy-orphaned-versions";

    public static final String INCLUDE_PATHS = "include-paths";

    public static final String EXCLUDE_PATHS = "exclude-paths";

    public static final String MERGE_PATHS = "merge-paths";

    public static OptionParser create() {
        OptionParser op = new OptionParser();
        addUsageOptions(op);
        addBlobOptions(op);
        addRdbOptions(op);
        addPathsOptions(op);
        addVersioningOptions(op);
        addMiscOptions(op);
        return op;
    }

    private static void addUsageOptions(OptionParser op) {
        op.acceptsAll(asList("h", "?", HELP), "show help").forHelp();
    }

    private static void addBlobOptions(OptionParser op) {
        op.accepts(COPY_BINARIES, "Copy binary content. Use this to disable use of existing DataStore in new repo");
        op.accepts(SRC_FDS, "Datastore directory to be used as a source FileDataStore").withRequiredArg()
                .ofType(String.class);
        op.accepts(SRC_FBS, "Datastore directory to be used as a source FileBlobStore").withRequiredArg()
                .ofType(String.class);
        op.accepts(SRC_S3, "Datastore directory to be used for the source S3").withRequiredArg().ofType(String.class);
        op.accepts(SRC_S3_CONFIG, "Configuration file for the source S3DataStore").withRequiredArg()
                .ofType(String.class);
        op.accepts(DST_FDS, "Datastore directory to be used as a target FileDataStore").withRequiredArg()
                .ofType(String.class);
        op.accepts(DST_FBS, "Datastore directory to be used as a target FileBlobStore").withRequiredArg()
                .ofType(String.class);
        op.accepts(DST_S3, "Datastore directory to be used for the target S3").withRequiredArg().ofType(String.class);
        op.accepts(DST_S3_CONFIG, "Configuration file for the target S3DataStore").withRequiredArg()
                .ofType(String.class);
    }

    private static void addRdbOptions(OptionParser op) {
        op.accepts(SRC_USER, "Source rdb user").withRequiredArg().ofType(String.class);
        op.accepts(SRC_PASSWORD, "Source rdb password").withRequiredArg().ofType(String.class);
        op.accepts(DST_USER, "Target rdb user").withRequiredArg().ofType(String.class);
        op.accepts(DST_PASSWORD, "Target rdb password").withRequiredArg().ofType(String.class);
    }

    private static void addPathsOptions(OptionParser op) {
        op.accepts(INCLUDE_PATHS, "Comma-separated list of paths to include during copy.").withRequiredArg()
                .ofType(String.class);
        op.accepts(EXCLUDE_PATHS, "Comma-separated list of paths to exclude during copy.").withRequiredArg()
                .ofType(String.class);
        op.accepts(MERGE_PATHS, "Comma-separated list of paths to merge during copy.").withRequiredArg()
                .ofType(String.class);
    }

    private static void addVersioningOptions(OptionParser op) {
        op.accepts(COPY_VERSIONS,
                "Copy the version storage. Parameters: { true | false | yyyy-mm-dd }. Defaults to true.")
                .withRequiredArg().ofType(String.class);
        op.accepts(COPY_ORPHANED_VERSIONS,
                "Allows to skip copying orphaned versions. Parameters: { true | false | yyyy-mm-dd }. Defaults to true.")
                .withRequiredArg().ofType(String.class);
    }

    private static void addMiscOptions(OptionParser op) {
        op.accepts(MMAP, "Enable memory mapped file access for Segment Store");
        op.accepts(FAIL_ON_ERROR, "Fail completely if nodes can't be read from the source repo");
        op.accepts(EARLY_SHUTDOWN,
                "Shutdown the source repository after nodes are copied and before the commit hooks are applied");
        op.accepts(CACHE_SIZE, "Cache size in MB").withRequiredArg().ofType(Integer.class).defaultsTo(256);
    }
}
