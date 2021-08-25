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

package org.apache.jackrabbit.oak.commons.jmx;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.jetbrains.annotations.NotNull;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Hashtable;
import java.util.Map;

/**
 * Utility methods related to JMX
 */
public final class JmxUtil {

    private JmxUtil() {
    }

    /**
     * Checks if the passed value string can be used as is as part of
     * JMX {@link javax.management.ObjectName} If it cannot be used then
     * it would return a quoted string which is then safe to be used
     * as part of ObjectName.
     *
     * <p>This is meant to avoid unnecessary quoting of value</p>
     *
     * @param unquotedValue to quote if required
     * @return passed value or quoted value if required
     */
    public static String quoteValueIfRequired(String unquotedValue) {
        String result;
        String quotedValue = ObjectName.quote(unquotedValue);

        //Check if some chars are escaped or not. In that case
        //length of quoted string (excluding quotes) would differ
        if (quotedValue.substring(1, quotedValue.length() - 1).equals(unquotedValue)) {
            ObjectName on = null;
            try {
                //Quoting logic in ObjectName does not escape ',', '='
                //etc. So try now by constructing ObjectName. If that
                //passes then value can be used as safely

                //Also we cannot just rely on ObjectName as it treats
                //*, ? as pattern chars and which should ideally be escaped
                on = new ObjectName("dummy", "dummy", unquotedValue);
            } catch (MalformedObjectNameException ignore) {
                //ignore
            }

            if (on != null){
                result = unquotedValue;
            } else {
                result = quotedValue;
            }
        } else {
            //Some escaping done. So do quote
            result = quotedValue;
        }
        return result;
    }

    /**
     * Constructs an immutable map with a single "jmx.objectname" key and a new {@link ObjectName} as value. The domain 
     * of the {@link ObjectName} will be {@link WhiteboardUtils#JMX_OAK_DOMAIN}.
     * Note that property values as well as the given type and name will get quoted according to 
     * {@link #quoteValueIfRequired(String)}.
     * 
     * @param type The type of bean
     * @param name The name of the bean
     * @param properties A map of additional properties
     * @return An immutable map with key "jmx.objectname" and a new {@link ObjectName} constructed from the parameters.
     * @throws MalformedObjectNameException If constructing the {@link ObjectName} fails.
     */
    @NotNull
    public static Map<String, ObjectName> createObjectNameMap(@NotNull String type, @NotNull String name,
                                                              @NotNull Map<String, String> properties) throws MalformedObjectNameException {
        Hashtable<String, String> table = new Hashtable<>();
        table.put("type", quoteValueIfRequired(type));
        table.put("name", quoteValueIfRequired(name));
        properties.forEach((key, value) -> table.put(key, quoteValueIfRequired(value)));
        return ImmutableMap.of("jmx.objectname", new ObjectName(WhiteboardUtils.JMX_OAK_DOMAIN, table));
    }
}
