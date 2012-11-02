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
package org.apache.jackrabbit.oak.plugins.index.old.mk.json.fast;

import java.math.BigDecimal;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopTokenizer;

/**
 * A Jsop object.
 */
public class Jsop {

    protected String jsop;
    protected int start, end;

    Jsop(String jsop, int start, int end) {
        this.jsop = jsop;
        this.start = start;
        this.end = end;
    }

    public static String toString(Object o) {
        if (o == null) {
            return "null";
        } else if (o instanceof String) {
            return JsopBuilder.encode((String) o);
        }
        return o.toString();
    }

    public static Object parse(String jsop) {
        return parse(jsop, 0);
    }

    private static Object parse(String jsop, int start) {
        if (jsop == null) {
            return null;
        }
       char c = jsop.charAt(start);
       switch(c) {
       case '{':
           return new JsopObject(jsop, start, Integer.MAX_VALUE);
       case '[':
           return new JsopArray(jsop, start, Integer.MAX_VALUE);
       case 'n':
           return null;
       case 't':
           return Boolean.TRUE;
       case 'f':
           return Boolean.FALSE;
       case '\"':
           return readString(jsop, start + 1);
       case '-':
           return readNumber(jsop, start);
       default:
           if (c >= '0' && c <= '9') {
               return readNumber(jsop, start);
           }
       }
       throw new IllegalArgumentException("Invalid jsop: " + jsop.substring(start));
    }

    private static String readString(String j, int start) {
        int pos = start;
        int length = j.length();
        boolean escaped = false;
        while (pos < length) {
            char c = j.charAt(pos++);
            if (c == '\\') {
                escaped = true;
                pos++;
            } else if (c == '"') {
                break;
            }
        }
        if (escaped) {
            return JsopTokenizer.decode(j.substring(start, pos - 1));
        }
        return j.substring(start, pos - 1);
    }

    private static BigDecimal readNumber(String j, int start) {
        int pos = start;
        int length = j.length();
        char ch = j.charAt(pos);
        if (ch == '-') {
            pos++;
        }
        while (true) {
            ch = j.charAt(pos);
            if (ch >= '0' && ch <= '9') {
                while (pos < length) {
                    ch = j.charAt(pos);
                    if (ch < '0' || ch > '9') {
                        break;
                    }
                    pos++;
                }
                if (ch == '.') {
                    pos++;
                    while (pos < length) {
                        ch = j.charAt(pos);
                        if (ch < '0' || ch > '9') {
                            break;
                        }
                        pos++;
                    }
                }
                if (ch == 'e' || ch == 'E') {
                    ch = j.charAt(++pos);
                    if (ch == '+' || ch == '-') {
                        ch = j.charAt(++pos);
                    }
                    while (pos < length) {
                        ch = j.charAt(pos);
                        if (ch < '0' || ch > '9') {
                            break;
                        }
                        pos++;
                    }
                }
                break;
            }
        }
        return new BigDecimal(j.substring(start, pos));
    }

}
