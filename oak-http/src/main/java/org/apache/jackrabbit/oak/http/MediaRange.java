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
package org.apache.jackrabbit.oak.http;

import java.util.HashMap;
import java.util.Map;

import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;

public class MediaRange {

    private final MediaType type;

    private final double q;

    public static MediaRange parse(String range, MediaTypeRegistry registry) {
        MediaType type = MediaType.parse(range);
        if (type == null) {
            return null;
        }
        type = registry.normalize(type);

        Map<String, String> parameters =
                new HashMap<String, String>(type.getParameters());
        String q = parameters.remove("q");
        if (q != null) {
            try {
                return new MediaRange(
                        new MediaType(type.getBaseType(), parameters),
                        Double.parseDouble(q));
            } catch (NumberFormatException e) {
                return null;
            }
        }

        return new MediaRange(type, 1.0);
    }

    public MediaRange(MediaType type, double q) {
        this.type = type;
        this.q = q;
    }

    public double match(MediaType type, MediaTypeRegistry registry) {
        if (type.equals(this.type)) { // shortcut
            return q;
        }

        for (Map.Entry<String, String> entry
                : this.type.getParameters().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (!value.equals(type.getParameters().get(key))) {
                return 0.0;
            }
        }

        if ("*/*".equals(this.type.toString())) {
            return q;
        } else if ("*".equals(this.type.getSubtype())
                && type.getType().equals(this.type.getType())) {
            return q;
        } else {
            return 0.0;
        }
    }

}
