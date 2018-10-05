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

import java.util.Set;

import joptsimple.OptionSet;

public interface OptionsBean {

    void configure(OptionSet options);

    /**
     * Title string for this set of options
     */
    String title();

    /**
     * Description string for this set of options
     */
    String description();

    /**
     * Used to sort the help output. Help for OptionsBean in descending order i.e.
     * bean having highest order would be rendered first
     */
    int order();

    /**
     * Option names which are actually performing operation
     */
    Set<String> operationNames();
}
