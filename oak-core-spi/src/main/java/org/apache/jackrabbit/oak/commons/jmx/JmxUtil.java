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

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

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


}
