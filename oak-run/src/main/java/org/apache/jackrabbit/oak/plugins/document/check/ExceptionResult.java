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
package org.apache.jackrabbit.oak.plugins.document.check;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * <code>ExceptionResult</code>...
 */
public class ExceptionResult implements Result {

    private final Throwable throwable;

    public ExceptionResult(@NotNull Throwable throwable) {
        this.throwable = throwable;
    }

    @Override
    public String toJson() {
        JsopBuilder json = new JsopBuilder();
        json.object();
        json.key("type").value("exception");
        json.key("exception").value(throwable.getClass().getName());
        json.key("message").value(throwable.getMessage());
        json.endObject();
        return json.toString();
    }
}
