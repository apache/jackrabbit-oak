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
package org.apache.jackrabbit.oak.plugins.observation.filter;

public class GlobbingPathHelper {

    public static String globAsRegex(String patternWithGlobs) {
        if (patternWithGlobs == null) {
            return null;
        } else if (!patternWithGlobs.contains("*")) {
            return patternWithGlobs;
        }
        String[] starStarParts = patternWithGlobs.split("\\*\\*", -1);
        StringBuffer sb = new StringBuffer();
        sb.append("\\Q");
        for (int i = 0; i < starStarParts.length; i++) {
            if (i > 0) {
                // the '**' regexp equivalent:
                // - anything not including '/'
                // - followed by ('/' plus anything not including '/') repeated
                sb.append("\\E[^/]*(/[^/]*)*\\Q");
            }
            String part = starStarParts[i];
            if (!part.equals("/")) {
                if (starStarParts.length > 1 && part.startsWith("/")) {
                    part = part.substring(1);
                }
                if (i < starStarParts.length -1 && part.endsWith("/")) {
                    part = part.substring(0, part.length() - 1);
                }
                sb.append(part.replace("*", "\\E[^/]*\\Q"));
            }
        }
        sb.append("\\E");
        return sb.toString();
    }

}
