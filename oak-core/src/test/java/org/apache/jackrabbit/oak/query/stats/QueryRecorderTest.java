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
package org.apache.jackrabbit.oak.query.stats;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class QueryRecorderTest {
    
    @Test
    public void simplify() {
        // SQL-2
        // dummy
        assertEquals("SELECT sling:alias FROM nt:base WHERE sling:alias IS NOT NULL", 
                QueryRecorder.simplify("SELECT sling:alias FROM nt:base WHERE sling:alias IS NOT NULL"));
        // replace strings and paths
        assertEquals("SELECT * FROM [acme] AS s WHERE ISDESCENDANTNODE('x') " + 
                "AND s.[sling:resourceType] = 'x'", 
                QueryRecorder.simplify("SELECT * FROM [acme] AS s WHERE ISDESCENDANTNODE([/conf]) " + 
                        "AND s.[sling:resourceType] = 'dam/123'"));
        
        // XPath
        // keep two path segment
        assertEquals("  /jcr:root//element(*,sling:Job)[@status='x'] order by @startTime descending", 
                QueryRecorder.simplify("  /jcr:root//element(*,sling:Job)[@status='RUNNING'] order by @startTime descending"));
        assertEquals("/jcr:root/content/element(*,sling:Job)[@status='x']", 
                QueryRecorder.simplify("/jcr:root/content/element(*,sling:Job)[@status='RUNNING']"));
        assertEquals("/jcr:root/content/abc/element(*,sling:Job)[@status='x']", 
                QueryRecorder.simplify("/jcr:root/content/abc/element(*,sling:Job)[@status='RUNNING']"));
        assertEquals("/jcr:root/content/abc/.../element(*, acme)[@status='x']", 
                QueryRecorder.simplify("/jcr:root/content/abc/def/element(*, acme)[@status='RUNNING']"));
        assertEquals("/jcr:root/content/abc/.../*[@status='x']", 
                QueryRecorder.simplify("/jcr:root/content/abc/def/*[@status='RUNNING']"));
        assertEquals("/jcr:root/content/*/jcr:content[@deviceIdentificationMode]", 
                QueryRecorder.simplify("/jcr:root/content/*/jcr:content[@deviceIdentificationMode]"));
        assertEquals("/jcr:root/(content|var)/b/.../*/jcr:content", 
                QueryRecorder.simplify("/jcr:root/(content|var)/b/c/*/jcr:content"));
        
    }

}
