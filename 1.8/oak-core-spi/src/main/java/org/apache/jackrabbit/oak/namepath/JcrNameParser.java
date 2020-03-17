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
package org.apache.jackrabbit.oak.namepath;

import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.util.XMLChar;

/**
 * Parses and validates JCR names. Upon successful completion of
 * {@link #parse(String, Listener, int)}
 * the specified listener is informed about the (resulting) JCR name.
 * In case of failure {@link JcrNameParser.Listener#error(String)} is called indicating
 * the reason.
 */
public final class JcrNameParser {

    // constants for parser
    private static final int STATE_PREFIX_START = 0;
    private static final int STATE_PREFIX = 1;
    private static final int STATE_NAME_START = 2;
    private static final int STATE_NAME = 3;
    private static final int STATE_URI_START = 4;
    private static final int STATE_URI = 5;

    /**
     * Listener interface for this name parser.
     */
    interface Listener {

        /**
         * Informs this listener that parsing the jcr name failed.
         *
         * @param message Details about the error.
         * @see JcrNameParser#parse(String, Listener, int)
         */
        void error(String message);

        /**
         * Informs this listener about the result of
         * {@link JcrNameParser#parse(String, Listener, int)}
         *
         * @param name The resulting name upon successful completion of
         * {@link JcrNameParser#parse(String, Listener, int)}
         * @param index the index (or {@code 0} when not specified)
         */
        boolean name(String name, int index);
    }

    /**
     * Avoid instantiation
     */
    private JcrNameParser() {
    }

    /**
     * Parse the specified jcr name and inform the specified {@code listener}
     * about the result or any error that may occur during parsing.
     *
     * @param jcrName The jcr name to be parsed.
     * @param listener The listener to be informed about success or failure.
     * @param index index, or {@code 0} when not specified
     * @return whether parsing was successful
     */
    public static boolean parse(String jcrName, Listener listener, int index) {
        // trivial check
        int len = jcrName == null ? 0 : jcrName.length();
        if (len == 0) {
            listener.error("Empty name");
            return false;
        }
        if (".".equals(jcrName) || "..".equals(jcrName)) {
            listener.error("Illegal name:" + jcrName);
            return false;
        }

        // parse the name
        String prefix;
        int nameStart = 0;
        int state = STATE_PREFIX_START;

        for (int i = 0; i < len; i++) {
            char c = jcrName.charAt(i);
            if (c == ':') {
                if (state == STATE_PREFIX_START) {
                    listener.error("Prefix must not be empty");
                    return false;
                } else if (state == STATE_PREFIX) {
                    prefix = jcrName.substring(0, i);
                    if (!XMLChar.isValidNCName(prefix)) {
                        listener.error("Invalid name prefix: "+ prefix);
                        return false;
                    }
                    state = STATE_NAME_START;
                } else if (state == STATE_URI) {
                    // ignore -> validation of uri later on.
                } else {
                    listener.error("'" + c + "' not allowed in name");
                    return false;
                }
            } else if (c == '[' || c == ']' || c == '*' || c == '|') {
                listener.error("'" + c + "' not allowed in name");
                return false;
            } else if (c == '/') {
                if (state == STATE_URI_START) {
                    state = STATE_URI;
                } else if (state != STATE_URI) {
                    listener.error("'" + c + "' not allowed in name");
                    return false;
                }
            } else if (c == '{') {
                if (state == STATE_PREFIX_START) {
                    state = STATE_URI_START;
                } else if (state == STATE_URI_START || state == STATE_URI) {
                    // second '{' in the uri-part -> no valid expanded jcr-name.
                    // therefore reset the nameStart and change state.
                    state = STATE_NAME;
                    nameStart = 0;
                } else if (state == STATE_NAME_START) {
                    state = STATE_NAME;
                    nameStart = i;
                }
            } else if (c == '}') {
                if (state == STATE_URI_START || state == STATE_URI) {
                    String tmp = jcrName.substring(1, i);
                    if (tmp.isEmpty() || tmp.indexOf(':') != -1) {
                        // The leading "{...}" part is empty or contains
                        // a colon, so we treat it as a valid namespace URI.
                        // More detailed validity checks (is it well formed,
                        // registered, etc.) are not needed here.
                        state = STATE_NAME_START;
                    } else if (tmp.equals("internal")) {
                        // As a special Jackrabbit backwards compatibility
                        // feature, support {internal} as a valid URI prefix
                        state = STATE_NAME_START;
                    } else if (tmp.indexOf('/') == -1) {
                        // The leading "{...}" contains neither a colon nor
                        // a slash, so we can interpret it as a a part of a
                        // normal local name.
                        state = STATE_NAME;
                        nameStart = 0;
                    } else {
                        listener.error("The URI prefix of the name " + jcrName + " is " +
                                "neither a valid URI nor a valid part of a local name.");
                        return false;
                    }
                } else if (state == STATE_PREFIX_START) {
                    state = STATE_PREFIX; // prefix start -> validation later on will fail.
                } else if (state == STATE_NAME_START) {
                    state = STATE_NAME;
                    nameStart = i;
                }
            } else {
                if (state == STATE_PREFIX_START) {
                    state = STATE_PREFIX; // prefix start
                } else if (state == STATE_NAME_START) {
                    state = STATE_NAME;
                    nameStart = i;
                } else if (state == STATE_URI_START) {
                    state = STATE_URI;
                }
            }
        }

        // take care of qualified jcrNames starting with '{' that are not having
        // a terminating '}' -> make sure there are no illegal characters present.
        if (state == STATE_URI && (jcrName.indexOf(':') > -1 || jcrName.indexOf('/') > -1)) {
            listener.error("Local name may not contain ':' nor '/'");
            return false;
        }

        if (nameStart == len || state == STATE_NAME_START) {
            listener.error("Local name must not be empty");
            return false;
        }

        return listener.name(jcrName, index);
    }

    public static boolean validate(String jcrName) {
        Listener listener = new Listener() {
            @Override
            public void error(String message) {
            }

            @Override
            public boolean name(String name, int index) {
                return true;
            }
        };
        return parse(jcrName, listener, 0);
    }

    public static void checkName(String jcrName, boolean allowResidual) throws ConstraintViolationException {
        if (jcrName == null || !(allowResidual && "*".equals(jcrName) || validate(jcrName))) {
            throw new ConstraintViolationException("Not a valid JCR name '" + jcrName + '\'');
        }
    }
}
