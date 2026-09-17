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
package org.apache.jackrabbit.oak.plugins.index.search;

/**
 * Reports index statistics (for example, how many entries does the index contain).
 */
public interface IndexStatistics {
  int numDocs();

  int getDocCountFor(String key);

  /**
   * Approximate number of indexed documents below the given ancestor path, using the index's own
   * path/depth data (rather than a global heuristic). Used to estimate the cost of
   * {@code ISCHILDNODE} / {@code ISDESCENDANTNODE} restrictions.
   *
   * @param ancestorPath the ancestor path (a {@code :ancestors} value), e.g. {@code /content}
   * @param exactDepth   when {@code >= 0}, count only nodes at exactly this depth (the direct
   *                     children of {@code ancestorPath}); when {@code < 0}, count all descendants
   * @return the approximate document count, or {@code -1} if this statistic is not available
   */
  default int getDocCountForPath(String ancestorPath, int exactDepth) {
    return -1;
  }
}
