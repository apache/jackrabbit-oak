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

import org.apache.jackrabbit.oak.index.ElasticIndexCommand;
import org.apache.jackrabbit.oak.index.ElasticPurgeOldIndexVersionCommand;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.Modes;

import java.util.Map;

/*
Avaialble modes for elastic. Add new elastic operations/commands to be supported here.
 */
public final class AvailableElasticModes {
    // list of available Modes for the tool
    public static final Modes MODES = new Modes(
            Map.of(
                    "index", new ElasticIndexCommand(),
                    "purge-index-versions", new ElasticPurgeOldIndexVersionCommand()));
}
