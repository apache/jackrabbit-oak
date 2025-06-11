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
package org.apache.jackrabbit.oak.run;

import java.util.HashSet;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.cli.OptionsBean;
import org.apache.jackrabbit.oak.run.cli.OptionsBeanFactory;

public class NamespaceRegistryOptions implements OptionsBean {

    public static final OptionsBeanFactory FACTORY = NamespaceRegistryOptions::new;

    private OptionSet options;
    private final Set<OptionSpec<Void>> actionOpts;
    private final Set<String> operationNames;

    private final OptionSpec<Void> analyseOpt;
    private final OptionSpec<Void> fixOpt;

    public NamespaceRegistryOptions(OptionParser parser) {
        analyseOpt = parser.accepts("analyse", "List the prefix to namespace map and check for consistency.");
        fixOpt = parser.accepts("fix", "List the prefix to namespace map, check for consistency and fix any inconsistencies, if possible.");
        actionOpts = Set.of(analyseOpt, fixOpt);
        operationNames = collectionOperationNames(actionOpts);
//        userOpt = parser
//                .accepts("user", "User name").withOptionalArg()
//                .defaultsTo("admin");
//        passwordOpt = parser
//                .accepts("password", "Password").withOptionalArg()
//                .defaultsTo("admin");
    }

    @Override
    public void configure(OptionSet options) {
        this.options = options;
    }

    @Override
    public String title() {
        return "";
    }

    @Override
    public String description() {
        return "The namespace-registry command supports the following operations.";
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Set<String> operationNames() {
        return operationNames;
    }

//    public String getUser() {
//        return userOpt.value(options);
//    }
//
//    public String getPassword() {
//        return passwordOpt.value(options);
//    }

    public boolean anyActionSelected() {
        for (OptionSpec spec : actionOpts) {
            if (options.has(spec)){
                return true;
            }
        }
        return false;
    }

    public boolean analyse() {
        return  options.has(analyseOpt);
    }

    public boolean fix() {
        return  options.has(fixOpt);
    }

    private static Set<String> collectionOperationNames(Set<OptionSpec<Void>> actionOpts) {
        Set<String> result = new HashSet<>();
        for (OptionSpec spec : actionOpts){
            result.addAll(spec.options());
        }
        return result;
    }
}
