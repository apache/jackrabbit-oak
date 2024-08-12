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

/**
 * This package contains classes related to lucene hybrid index v2 where the index content
 * is stored using both property index (for recent enrties) and lucene indexes (for older entries). Related
 * document can be found
 * <a href="http://jackrabbit.apache.org/archive/wiki/JCR/attachments/115513516/115513517.pdf">here</a>.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import org.osgi.annotation.versioning.Version;