/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.optimizer;

import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;

public class PolishToQueryConverter {

    /**
     * Converts a given Polish notation string to either XPath or JCR-SQL2 syntax based on the
     * specified flag.
     *
     * @param polishNotation The Polish notation string to be converted.
     * @param isXPath        A boolean flag indicating whether to convert to XPath (true) or
     *                       JCR-SQL2 (false) syntax.
     * @return A string representing the converted query in either XPath or JCR-SQL2 syntax.
     */
    public static String apply(String polishNotation, boolean isXPath) {
        Deque<String> tokens = new LinkedList<>(Arrays.asList(polishNotation.split("\\*")));
        if ("function".equals(tokens.peek())) {
            tokens.poll();
        }
        return parseTokens(tokens, isXPath);
    }

    /**
     * Recursively parses tokens from a deque representing a Polish notation expression and converts
     * them into either XPath or JCR-SQL2 query syntax. We use a deque, as we can tokenize each part
     * of the expression as they are separated by "*".
     *
     * @param tokens  A deque of tokens derived from the Polish notation expression.
     * @param isXPath A boolean flag indicating whether to convert to XPath (true) or JCR-SQL2
     *                (false) syntax.
     * @return A string representing the converted part of the query in the appropriate syntax.
     */
    private static String parseTokens(Deque<String> tokens, boolean isXPath) {
        if (tokens.isEmpty()) {
            return "";
        }

        String token = tokens.poll();
        String fn;

        switch (token) {
            case "upper":
                fn = isXPath ? "fn:upper-case(" : "upper(";
                return fn + parseTokens(tokens, isXPath) + ")";
            case "lower":
                fn = isXPath ? "fn:lower-case(" : "lower(";
                return fn + parseTokens(tokens, isXPath) + ")";
            case "coalesce":
                fn = isXPath ? "fn:coalesce(" : "coalesce(";
                return fn + parseTokens(tokens, isXPath) + "," + parseTokens(tokens, isXPath) + ")";
            case "first":
                fn = isXPath ? "jcr:first(" : "first(";
                return fn + parseTokens(tokens, isXPath) + ")";
            case "length":
                fn = isXPath ? "fn:string-length(" : "length(";
                return fn + parseTokens(tokens, isXPath) + ")";
            case "@:localname":
                return isXPath ? "fn:local-name()" : "localname()";
            case "@:name":
                return isXPath ? "fn:name()" : "name()";
            case "@:path":
                return isXPath ? "fn:path()" : "path()";
            // Handle properties
            default:
                return isXPath ? formatXPathProperty(token) : formatSQL2Property(token);
        }
    }

    /**
     * Properties in JCR-SQL2 needs to be surrounded with [ ] and doesn't have "@".
     * Also, ] is escaped as ]].
     */
    private static String formatSQL2Property(String token) {
        if (token.startsWith("@")) {
            String property = token.substring(1);
            property = property.replaceAll("]", "]]");
            return "[" + property + "]";
        }
        return token;
    }

    /**
     * This method formats properties from Polish notation to valid XPath. The property tokens are
     * always prefixed with "@". Since the token might contain a "/", meaning a nested property, we
     * need to format it to a valid XPath which means that the "deepest" child needs to be prefixed
     * with "@" instead. Example: "@jcr:content/foo/bar/property1" to
     * "jcr:content/foo/bar/@property1".
     *
     * @param token The property token in Polish notation.
     * @return The valid XPath formatted property.
     */
    private static String formatXPathProperty(String token) {
        if (token.contains("/")) {
            token = token.substring(token.indexOf("@") + 1);
            int lastSlash = token.lastIndexOf('/');
            return token.substring(0, lastSlash) + "/@" + token.substring(lastSlash + 1);
        }
        return token;
    }
}

