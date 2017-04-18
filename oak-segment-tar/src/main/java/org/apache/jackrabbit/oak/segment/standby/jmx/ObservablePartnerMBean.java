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

import org.apache.jackrabbit.oak.api.jmx.Description;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public interface ObservablePartnerMBean {

    @Nonnull
    @Description("name of the partner")
    String getName();

    @Description("IP of the remote")
    String getRemoteAddress();

    @Description("Last request")
    String getLastRequest();

    @Description("Port of the remote")
    int getRemotePort();

    @CheckForNull
    @Description("Time the remote instance was last contacted")
    String getLastSeenTimestamp();

    @Description("Number of transferred segments")
    long getTransferredSegments();

    @Description("Number of bytes stored in transferred segments")
    long getTransferredSegmentBytes();

    @Description("Number of transferred binaries")
    long getTransferredBinaries();

    @Description("Number of bytes stored in transferred binaries")
    long getTransferredBinariesBytes();

}
