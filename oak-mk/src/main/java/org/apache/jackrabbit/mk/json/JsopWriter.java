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
package org.apache.jackrabbit.mk.json;

public interface JsopWriter {

    JsopWriter array();

    JsopWriter object();

    JsopWriter key(String key);

    JsopWriter value(String value);

    JsopWriter encodedValue(String raw);

    JsopWriter endObject();

    JsopWriter endArray();

    JsopWriter tag(char tag);

    JsopWriter append(JsopWriter diff);

    JsopWriter value(long x);

    JsopWriter value(boolean b);

    JsopWriter newline();

    void resetWriter();

    void setLineLength(int i);

}
