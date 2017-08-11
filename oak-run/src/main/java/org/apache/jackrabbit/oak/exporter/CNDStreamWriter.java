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

package org.apache.jackrabbit.oak.exporter;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Writes nodes in CND format
 *
 * <pre>
 *       + projects
 *          - jcr:primaryType = "sling:OrderedFolder"
 *          - jcr:mixinTypes = ["rep:AccessControllable"]
 *          - jcr:createdBy = "admin"
 *          - jcr:created = "2017-01-26T08:02:08.602+05:30"
 *          - sling:resourceType = "sling/projects"
 *          + rep:policy
 *            - jcr:primaryType = "rep:ACL"
 * </pre>
 */
class CNDStreamWriter implements JsopWriter, Closeable {
    private static final Set<String> COMMON_TYPE_CODES = ImmutableSet.of("nam:", "dat:");
    private enum State {NONE, STARTED, BEGIN, END}
    private final PrintWriter w;
    private State arrayState = State.NONE;
    private int depth = 0;
    private String deferredName;
    private String indent = "  ";

    public CNDStreamWriter(PrintWriter w) {
        this.w = w;
    }

    @Override
    public JsopWriter object() {
        if (deferredName != null) {
            w.println();
            space();
            w.print('+');
            w.print(' ');
            w.print(deferredName);
            deferredName = null;
        }
        depth++;
        return this;
    }

    @Override
    public JsopWriter endObject() {
        optionalResetArrayState();
        depth--;
        return this;
    }

    @Override
    public JsopWriter array() {
        checkState(arrayState == State.NONE);
        optionalKey();
        arrayState = State.BEGIN;
        w.append('[');
        return this;
    }

    @Override
    public JsopWriter endArray() {
        checkState(arrayState == State.BEGIN || arrayState == State.STARTED);
        arrayState = State.END;
        w.append(']');
        return this;
    }


    @Override
    public JsopWriter key(String key) {
        optionalResetArrayState();
        deferredName = key;
        return this;
    }

    @Override
    public JsopWriter encodedValue(String raw) {
        optionalKey();
        optionalComma();
        w.print(raw);
        optionalChangeArrayState();
        return this;
    }

    @Override
    public JsopWriter value(String value) {
        return encodedValue(JsopBuilder.encode(stripTypeCode(value)));
    }

    @Override
    public JsopWriter value(long x) {
        return encodedValue(String.valueOf(x));
    }

    @Override
    public JsopWriter value(boolean b) {
        return encodedValue(String.valueOf(b));
    }

    @Override
    public void close() throws IOException {
        checkState(depth == 0);
    }

    private void optionalComma() {
        if (arrayState == State.STARTED) {
            w.print(',');
        }
    }

    private void optionalChangeArrayState() {
        if (arrayState == State.BEGIN) {
            arrayState = State.STARTED;
        }
    }

    private void optionalKey() {
        if (arrayState == State.BEGIN || arrayState == State.STARTED) {
            return;
        }
        checkNotNull(deferredName);
        w.println();
        space();
        w.print('-');
        w.print(' ');
        w.print(deferredName);
        w.print(" = ");
        deferredName = null;
    }

    private void space() {
        w.print(Strings.repeat(indent, depth));
    }

    private void optionalResetArrayState() {
        //Check that not within array
        checkState(arrayState == State.END || arrayState == State.NONE);
        if (arrayState == State.END) {
            arrayState = State.NONE;
        }
    }

    private static String stripTypeCode(String value) {
        if (value != null) {
            for (String code : COMMON_TYPE_CODES){
                if (value.startsWith(code)){
                    return value.substring(code.length());
                }
            }
        }
        return value;
    }

    //Unsupported operation. These are also not used by JsonSerializer

    @Override
    public JsopWriter tag(char tag) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsopWriter append(JsopWriter diff) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsopWriter newline() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetWriter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLineLength(int length) {
        throw new UnsupportedOperationException();
    }
}
