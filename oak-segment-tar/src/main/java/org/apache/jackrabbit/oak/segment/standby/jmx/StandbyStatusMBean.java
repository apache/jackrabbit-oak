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

package org.apache.jackrabbit.oak.segment.standby.jmx;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.jmx.Description;

public interface StandbyStatusMBean {
    String JMX_NAME = "org.apache.jackrabbit.oak:name=Status,type=\"Standby\"";
    String STATUS_INITIALIZING = "initializing";
    String STATUS_STOPPED = "stopped";
    String STATUS_STARTING = "starting";
    String STATUS_RUNNING = "running";
    String STATUS_CLOSING = "closing";
    String STATUS_CLOSED = "closed";

    @Nonnull
    @Description("primary or standby")
    String getMode();

    @Description("current status of the service")
    String getStatus();

    @Description("instance is running")
    boolean isRunning();

    @Description("stop the communication")
    void stop();

    @Description("start the communication")
    void start();
}
