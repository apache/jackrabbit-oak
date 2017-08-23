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

import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionSet;

public final class MigrationCliArguments {

    private final OptionSet options;

    private final List<String> arguments;

    public MigrationCliArguments(OptionSet options) throws CliArgumentException {
        this.options = options;
        this.arguments = getNonOptionArguments();
    }

    private List<String> getNonOptionArguments() {
        List<String> args = new ArrayList<String>();
        for (Object o : options.nonOptionArguments()) {
            args.add(o.toString());
        }
        return args;
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

    public Boolean getBooleanOption(String optionName) {
        return (Boolean) options.valueOf(optionName);
    }

    public String[] getOptionList(String optionName) {
        String option = getOption(optionName);
        if (option == null) {
            return null;
        } else {
            return option.split(",");
        }
    }

    public List<String> getArguments() {
        return arguments;
    }
}
