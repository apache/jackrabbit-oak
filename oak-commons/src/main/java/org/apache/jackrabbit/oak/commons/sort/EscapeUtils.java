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

package org.apache.jackrabbit.oak.commons.sort;

import javax.annotation.Nullable;


import static com.google.common.base.Preconditions.checkState;

/**
 * Utility class to escape '\n', '\r', '\' char
 * while being written to file and unescape then upon getting
 * read from file. This is used by StringSort and ExternalSort
 * to handle data which contains line break. If left unescaped
 * then such data interferes with the processing of such utilities
 */
public abstract class EscapeUtils {

    public static String escapeLineBreak(@Nullable String line) {
        if (line == null) {
            return null;
        }
        if (escapingRequired(line)) {
            line = escape(line);
        }
        return line;
    }

    public static String unescapeLineBreaks(@Nullable String line) {
        if (line == null) {
            return null;
        }
        if (unescapingRequired(line)) {
            line = unescape(line);
        }
        return line;
    }

    private static boolean escapingRequired(String line) {
        int len = line.length();
        for (int i = 0; i < len; i++) {
            char c = line.charAt(i);
            switch (c) {
                case '\n':
                case '\r':
                case '\\':
                    return true;
            }
        }
        return false;
    }

    private static boolean unescapingRequired(String line) {
        return line.indexOf('\\') >= 0;
    }

    private static String escape(String line) {
        int len = line.length();
        StringBuilder sb = new StringBuilder(len + 1);
        for (int i = 0; i < len; i++) {
            char c = line.charAt(i);
            /*
              Here we need not worry about unicode chars
              because UTF-16 represents supplementary characters using
              code units whose values are not used for BMP characters
              i.e. ASCII chars
             */
            switch (c) {
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String unescape(String line) {
        int len = line.length();
        StringBuilder sb = new StringBuilder(len - 1);
        for (int i = 0; i < len; i++) {
            char c = line.charAt(i);
            if (c == '\\') {
                checkState(i < len - 1, "Expected one more char after '\\' at [%s] in [%s]", i, line);
                char nextChar = line.charAt(i + 1);
                switch (nextChar) {
                    case 'n':
                        sb.append('\n');
                        i++;
                        break;
                    case 'r':
                        sb.append('\r');
                        i++;
                        break;
                    case '\\':
                        sb.append('\\');
                        i++;
                        break;
                    default:
                        String msg = String.format("Unexpected char [%c] found at %d of [%s]. " +
                                "Expected '\\' or 'r' or 'n", nextChar, i, line);
                        throw new IllegalArgumentException(msg);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

}
