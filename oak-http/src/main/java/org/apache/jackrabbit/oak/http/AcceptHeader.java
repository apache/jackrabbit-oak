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

import java.util.ArrayList;
import java.util.List;

import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;

public class AcceptHeader {

    private static final MediaTypeRegistry registry =
            MediaTypeRegistry.getDefaultRegistry();

    private final List<MediaRange> ranges = new ArrayList<MediaRange>();

    public AcceptHeader(String accept) {
        if (accept == null) {
            ranges.add(new MediaRange(MediaType.parse("*/*"), 1.0));
        } else {
            for (String part : accept.split("(\\s*,)+\\s*")) {
                MediaRange range = MediaRange.parse(part, registry); 
                if (range != null) {
                    ranges.add(range);
                }
            }
        }

    }

    public String resolve(String... types) {
        int maxIndex = 0;
        double[] qs = new double[types.length];
        for (int i = 0; i < types.length; i++) {
            MediaType type = registry.normalize(MediaType.parse(types[i]));
            for (MediaRange range : ranges) {
                qs[i] = Math.max(qs[i], range.match(type, registry));
            }
            if (qs[i] > qs[maxIndex]) {
                maxIndex = i;
            }
        }
        if (qs[maxIndex] > 0.0) {
            return types[maxIndex];
        } else {
            return MediaType.OCTET_STREAM.toString();
        }
    }

}
