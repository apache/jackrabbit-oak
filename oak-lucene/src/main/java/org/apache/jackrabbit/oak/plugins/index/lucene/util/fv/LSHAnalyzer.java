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
package org.apache.jackrabbit.oak.plugins.index.lucene.util.fv;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for LSH search
 */
public class LSHAnalyzer extends Analyzer {

  private static final int DEFAULT_SHINGLE_SIZE = 5;

  private final int min;
  private final int max;
  private final int hashCount;
  private final int bucketCount;
  private final int hashSetSize;

  private LSHAnalyzer(int min, int max, int hashCount, int bucketCount, int hashSetSize) {
    super();
    this.min = min;
    this.max = max;
    this.hashCount = hashCount;
    this.bucketCount = bucketCount;
    this.hashSetSize = hashSetSize;
  }

  public LSHAnalyzer() {
    this(DEFAULT_SHINGLE_SIZE, DEFAULT_SHINGLE_SIZE, MinHashFilter.DEFAULT_HASH_COUNT, MinHashFilter.DEFAULT_BUCKET_COUNT, MinHashFilter.DEFAULT_HASH_SET_SIZE);
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
    Tokenizer source = new FVTokenizer(Version.LUCENE_47, reader);
    TokenFilter truncate = new TruncateTokenFilter(source, 3);
    TokenFilter featurePos = new FeaturePositionTokenFilter(truncate);
    ShingleFilter shingleFilter = new ShingleFilter(featurePos, min, max);
    shingleFilter.setTokenSeparator(" ");
    shingleFilter.setOutputUnigrams(false);
    shingleFilter.setOutputUnigramsIfNoShingles(false);
    TokenStream filter = new MinHashFilter(shingleFilter, hashCount, bucketCount, hashSetSize, bucketCount > 1);
    return new TokenStreamComponents(source, filter);
  }

}