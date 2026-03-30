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
package org.apache.jackrabbit.oak.cache.api.impl.caffeine;

import java.util.concurrent.ExecutionException; /**
 * Internal wrapper used to tunnel checked loader failures through Caffeine's
 * unchecked loader callbacks before restoring them as {@link ExecutionException}
 * on the Oak-visible API surface.
 *
 * <p>TODO OAK-TASK16: per {@code TASKS.md}, remove this helper in TASK-16 once
 * checked-exception compatibility is no longer required on top of Caffeine.</p>
 */
public class CacheComputationException extends RuntimeException {

    public CacheComputationException(Throwable cause) {
        super(cause);
    }
}
