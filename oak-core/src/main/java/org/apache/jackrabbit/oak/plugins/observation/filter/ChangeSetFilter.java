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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import org.apache.jackrabbit.oak.spi.observation.ChangeSet;

/**
 * A ChangeSetFilter is capable of inspecting a ChangeSet
 * and deciding if the corresponding consumer
 * (eg EventListener) is possibly interested in it
 * or definitely not.
 * <p>
 * Falsely deciding to include is fine, falsely
 * deciding to exclude is not.
 */
public interface ChangeSetFilter {

    /**
     * Decides if the commit belonging to the provided
     * ChangeSet is potentially relevant to the listener
     * or if it can definitely be excluded.
     */
	boolean excludes(ChangeSet changeSet);

}
