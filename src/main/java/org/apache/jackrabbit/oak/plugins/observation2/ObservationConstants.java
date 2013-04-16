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

package org.apache.jackrabbit.oak.plugins.observation2;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;

/**
 * TODO document
 */
public interface ObservationConstants {
    String REP_OBSERVATION = "rep:observation";
    String LISTENERS = "listeners";
    String EVENTS = "events";

    String USER_DATA = "userData";
    String USER_ID = "userId";

    String TYPE = "type";
    String PATH = "path";
    String DATE = "date";
    String DEEP = "deep";
    String UUID = "uuid";
    String NODE_TYPES = "nodeTypes";
    String NO_LOCAL = "noLocal";

    String LISTENER_PATH = '/' + JCR_SYSTEM + '/' + REP_OBSERVATION + '/' + LISTENERS;
    String EVENTS_PATH = '/' + JCR_SYSTEM + '/' + REP_OBSERVATION + '/' + EVENTS;
}
