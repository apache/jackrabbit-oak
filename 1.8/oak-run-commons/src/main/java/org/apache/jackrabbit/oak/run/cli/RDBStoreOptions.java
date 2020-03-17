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
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class RDBStoreOptions implements OptionsBean {
    private final OptionSpec<String> rdbjdbcuser;
    private final OptionSpec<String> rdbjdbcpasswd;
    private OptionSet options;

    public RDBStoreOptions(OptionParser parser){
        rdbjdbcuser = parser.accepts("rdbjdbcuser", "RDB JDBC user").withOptionalArg().defaultsTo("");
        rdbjdbcpasswd = parser.accepts("rdbjdbcpasswd", "RDB JDBC password").withOptionalArg().defaultsTo("");
    }

    @Override
    public void configure(OptionSet options) {
        this.options = options;
    }

    @Override
    public String title() {
        return "RDBDocumentStore Options";
    }

    @Override
    public int order() {
        return 15;
    }

    @Override
    public String description() {
        return "Options related to configuring a RDBDocumentStore for DocumentNodeStore based setups.";
    }

    @Override
    public Set<String> operationNames() {
        return Collections.emptySet();
    }

    public String getUser(){
        return rdbjdbcuser.value(options);
    }

    public String getPassword(){
        return rdbjdbcpasswd.value(options);
    }
}
