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
package org.apache.jackrabbit.oak.jcr.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Item;
import javax.jcr.RepositoryException;

/**
 * {@code LogUtil}...
 */
public class LogUtil {

    private static Logger log = LoggerFactory.getLogger(LogUtil.class);

    /**
     * Avoid instantiation
     */
    private LogUtil() {}

    /**
     * Fail safe retrieval of the JCR path for a given item. This is intended
     * to be used in log output, error messages etc.
     *
     * @param item The target item.
     * @return The JCR path of that item or some implementation specific
     * string representation of the item.
     */
    public static String safeGetJCRPath(Item item) {
        try {
            return item.getPath();
        } catch (RepositoryException e) {
            log.error("Failed to retrieve path from item.");
            // return string representation of the item as a fallback
            return item.toString();
        }
    }

}