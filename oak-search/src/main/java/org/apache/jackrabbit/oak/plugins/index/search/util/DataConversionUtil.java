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
package org.apache.jackrabbit.oak.plugins.index.search.util;

import org.apache.jackrabbit.util.ISO8601;

/**
 * Utility class to convert data from one to another format.
 */
public class DataConversionUtil {

    /**
     * Date values are saved with sec resolution
     * @param date jcr data string
     * @return date value in seconds
     */
    public static Long dateToLong(String date){
        if( date == null){
            return null;
        }
        //TODO OAK-2204 - Should we change the precision to lower resolution
        return ISO8601.parse(date).getTimeInMillis();
    }

}

