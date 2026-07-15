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
package org.apache.jackrabbit.oak.segment.osgi;

final class DeprecationMessage {

    private static final String MOVED_PID_FORMAT = "Deprecated configuration detected!\n\n" +
            "  A configuration for %s\n" +
            "  was detected. The oak-segment bundle used to contain this component,\n" +
            "  but the bundle is now deprecated and should not be included in your\n" +
            "  deployment. The oak-segment-tar bundle exposes an equivalent and improved\n" +
            "  functionality but you need to rename your configuration to target the\n" +
            "  new component using the PID %s.\n";

    private DeprecationMessage() {}

    static String movedPid(String oldPid, String newPid) {
        return String.format(MOVED_PID_FORMAT, oldPid, newPid);
    }

}
