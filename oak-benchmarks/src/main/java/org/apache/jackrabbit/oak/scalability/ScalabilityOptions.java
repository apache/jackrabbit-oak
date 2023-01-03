/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.scalability;


import joptsimple.OptionParser;
import joptsimple.OptionSpec;

import java.io.File;

import static java.util.Arrays.asList;

public class ScalabilityOptions {

    private final OptionSpec<File> base;
    private final OptionSpec<String> host;
    private final OptionSpec<Integer> port;
    private final OptionSpec<String> dbName;
    private final OptionSpec<Boolean> dropDBAfterTest;
    private final OptionSpec<String> rdbjdbcuri;
    private final OptionSpec<String> rdbjdbcuser;
    private final OptionSpec<String> rdbjdbcpasswd;
    private final OptionSpec<String> rdbjdbctableprefix;
    private final  OptionSpec<Boolean> mmap;
    private final OptionSpec<Integer> cache;
    private final OptionSpec<Integer> fdsCache;
    private final OptionSpec<Boolean> withStorage;
    private final OptionSpec<Integer> coldSyncInterval;
    private final OptionSpec<Boolean> coldUseDataStore;
    private final OptionSpec<Boolean> coldShareDataStore;
    private final OptionSpec<Boolean> coldOneShotRun;
    private final OptionSpec<Boolean> coldSecure;
    private final OptionSpec<?> help;
    private final OptionSpec<String> nonOption;
    private final OptionSpec<File> csvFile;
    private final OptionSpec<Boolean> throttlingEnabled;

    public OptionSpec<Integer> getColdSyncInterval() {
        return coldSyncInterval;
    }

    public OptionSpec<File> getBase() {
        return base;
    }

    public OptionSpec<String> getHost() {
        return host;
    }

    public OptionSpec<Integer> getPort() {
        return port;
    }

    public OptionSpec<String> getDbName() {
        return dbName;
    }

    public OptionSpec<Boolean> getDropDBAfterTest() {
        return dropDBAfterTest;
    }

    public OptionSpec<String> getRdbjdbcuri() {
        return rdbjdbcuri;
    }

    public OptionSpec<String> getRdbjdbcuser() {
        return rdbjdbcuser;
    }

    public OptionSpec<String> getRdbjdbcpasswd() {
        return rdbjdbcpasswd;
    }

    public OptionSpec<String> getRdbjdbctableprefix() {
        return rdbjdbctableprefix;
    }

    public OptionSpec<Boolean> getMmap() {
        return mmap;
    }

    public OptionSpec<Integer> getCache() {
        return cache;
    }

    public OptionSpec<Integer> getFdsCache() {
        return fdsCache;
    }

    public OptionSpec<Boolean> getWithStorage() {
        return withStorage;
    }

    public OptionSpec<Boolean> getColdUseDataStore() {
        return coldUseDataStore;
    }

    public OptionSpec<Boolean> getColdShareDataStore() {
        return coldShareDataStore;
    }

    public OptionSpec<Boolean> getColdOneShotRun() {
        return coldOneShotRun;
    }

    public OptionSpec<Boolean> getColdSecure() {
        return coldSecure;
    }

    public OptionSpec<?> getHelp() {
        return help;
    }

    public OptionSpec<String> getNonOption() {
        return nonOption;
    }

    public OptionSpec<File> getCsvFile() {
        return csvFile;
    }

    public OptionSpec<Boolean> isThrottlingEnabled() {
        return throttlingEnabled;
    }



    public ScalabilityOptions(OptionParser parser) {

        base = parser.accepts("base", "Base directory")
                .withRequiredArg().ofType(File.class)
                .defaultsTo(new File("target"));
        host = parser.accepts("host", "MongoDB host")
                .withRequiredArg().defaultsTo("localhost");
        port = parser.accepts("port", "MongoDB port")
                .withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        dbName = parser.accepts("db", "MongoDB database")
                .withRequiredArg();
        dropDBAfterTest =
                parser.accepts("dropDBAfterTest",
                        "Whether to drop the MongoDB database after the test")
                        .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
        rdbjdbcuri = parser.accepts("rdbjdbcuri", "RDB JDBC URI")
                .withOptionalArg().defaultsTo("jdbc:h2:./target/benchmark");
        rdbjdbcuser = parser.accepts("rdbjdbcuser", "RDB JDBC user")
                .withOptionalArg().defaultsTo("");
        rdbjdbcpasswd = parser.accepts("rdbjdbcpasswd", "RDB JDBC password")
                .withOptionalArg().defaultsTo("");
        rdbjdbctableprefix = parser.accepts("rdbjdbctableprefix", "RDB JDBC table prefix")
                .withOptionalArg().defaultsTo("");
        mmap = parser.accepts("mmap", "TarMK memory mapping")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo("64".equals(System.getProperty("sun.arch.data.model")));
        cache = parser.accepts("cache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(100);
        fdsCache = parser.accepts("blobCache", "cache size (MB)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(32);
        withStorage = parser
                .accepts("storage", "Index storage enabled").withOptionalArg()
                .ofType(Boolean.class);
        csvFile =
                parser.accepts("csvFile", "File to write a CSV version of the benchmark data.")
                        .withOptionalArg().ofType(File.class);
        coldSyncInterval = parser.accepts("coldSyncInterval", "interval between sync cycles in sec (Segment-Tar-Cold only)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(5);
        coldUseDataStore = parser
                .accepts("useDataStore",
                        "Whether to use a datastore in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.TRUE);
        coldShareDataStore = parser
                .accepts("shareDataStore",
                        "Whether to share the datastore for primary and standby in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        coldOneShotRun = parser
                .accepts("oneShotRun",
                        "Whether to do a continuous sync between client and server or sync only once (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.TRUE);
        coldSecure = parser
                .accepts("secure",
                        "Whether to enable secure communication between primary and standby in the cold standby topology (Segment-Tar-Cold only)")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE);
        throttlingEnabled = parser
                .accepts("throttlingEnabled", "Whether throttling for Document Store is enabled or not")
                .withOptionalArg().ofType(Boolean.class)
                .defaultsTo(Boolean.FALSE); // throttling is disabled by default

        help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        nonOption = parser.nonOptions();


    }

}



