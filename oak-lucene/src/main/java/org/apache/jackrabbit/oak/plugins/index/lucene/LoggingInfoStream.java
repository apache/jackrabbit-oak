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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import org.apache.lucene.util.InfoStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingInfoStream extends InfoStream {

    static final String PREFIX = "oak.lucene";

    public static final LoggingInfoStream INSTANCE = new LoggingInfoStream();

    private LoggingInfoStream() {

    }

    @Override
    public void message(String component, String message) {
        getLog(component).debug("[{}] {}", component, message);
    }

    @Override
    public boolean isEnabled(String component) {
        return getLog(component).isDebugEnabled();
    }

    @Override
    public void close() throws IOException {

    }

    private static Logger getLog(String component) {
        return LoggerFactory.getLogger(PREFIX + "." + component);
    }
}
