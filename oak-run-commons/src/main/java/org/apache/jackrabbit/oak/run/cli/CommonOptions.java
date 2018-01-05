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

package org.apache.jackrabbit.oak.run.cli;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.util.Arrays.asList;

public class CommonOptions implements OptionsBean {
    public static final String DEFAULT_CONNECTION_STRING = "{<path-to-repository> | <mongodb-uri>} | <rdb-uri> | memory}";
    private final OptionSpec<Void> help;
    private final OptionSpec<Void> readWriteOption;
    private final OptionSpec<String> nonOption;
    private final OptionSpec<Void> metrics;
    private final OptionSpec<Void> segment;
    private OptionSet options;

    public CommonOptions(OptionParser parser){
        help = parser.acceptsAll(asList("h", "?", "help"), "Show help").forHelp();
        readWriteOption = parser.accepts("read-write", "Connect to repository in read-write mode");
        metrics = parser.accepts("metrics", "Enables metrics based statistics collection");
        segment = parser.accepts("segment", "Use older oak-segment support");
        nonOption = parser.nonOptions(DEFAULT_CONNECTION_STRING);
    }

    @Override
    public void configure(OptionSet options) {
        this.options = options;
    }

    public boolean isHelpRequested(){
        return options.has(help);
    }

    public boolean isReadWrite(){
        return options.has(readWriteOption);
    }

    public List<String> getNonOptions(){
        return nonOption.values(options);
    }

    public boolean isMongo(){
        return getStoreArg().startsWith("mongodb://");
    }

    public boolean isRDB(){
        return getStoreArg().startsWith("jdbc");
    }

    public boolean isOldSegment(){
        return options.has(segment);
    }

    public boolean isDocument(){
        return isMongo() || isRDB();
    }

    public boolean isMemory(){
        return getStoreArg().equalsIgnoreCase("memory");
    }

    public boolean isMetricsEnabled() {
        return options.has(metrics);
    }

    public String getStoreArg() {
        List<String> nonOptions = nonOption.values(options);
        return nonOptions.size() > 0 ? nonOptions.get(0) : "";
    }

    @Override
    public String title() {
        return "Global Options";
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public Set<String> operationNames() {
        return Collections.emptySet();
    }
}
