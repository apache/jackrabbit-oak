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

import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.util.Arrays.asList;

public class CommonOptions implements OptionsBean {
    private final OptionSpec<Void> help;
    private final OptionSpec<Void> readWriteOption;
    private final OptionSpec<String> nonOption;
    private OptionSet options;

    public CommonOptions(OptionParser parser){
        help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        readWriteOption = parser.accepts("read-write", "connect to repository in read-write mode");
        nonOption = parser.nonOptions("{<path-to-repository> | <mongodb-uri>}");
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

    public boolean isSegment(){
        return !isMongo() && !isRDB();
    }

    public String getStoreArg() {
        List<String> nonOptions = nonOption.values(options);
        return nonOptions.size() > 0 ? nonOptions.get(0) : "";
    }
}
