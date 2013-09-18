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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.util.Arrays;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;

public class OakWordTokenFilter extends CompoundWordTokenFilterBase {

    private static final String ALPHANUM_TYPE = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.ALPHANUM];

    private static final char[] SEPARATORS = new char[] { '_', '.' };

    private final char[] separators;
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

    public OakWordTokenFilter(Version version, TokenStream in, char[] separators) {
        super(version, in, null);
        this.separators = separators;
        Arrays.sort(this.separators);
    }

    public OakWordTokenFilter(Version version, TokenStream in) {
        this(version, in, SEPARATORS);
    }

    @Override
    protected void decompose() {
        if (ALPHANUM_TYPE.equals(typeAtt.type())) {
            final int len = termAtt.length();
            char[] buffer = termAtt.buffer();
            int tokenLen = 0;
            boolean foundOne = false;
            for (int i = 0; i < len; i++) {
                if (Arrays.binarySearch(separators, buffer[i]) >= 0) {
                    foundOne = true;
                    if (tokenLen > 0) {
                        CompoundToken ct = new CompoundToken(i - tokenLen,
                                tokenLen);
                        tokens.add(ct);
                    }
                    tokenLen = 0;
                } else {
                    tokenLen++;
                }
            }
            // if there's no split, don't return anything, let the parent
            // tokenizer return the full token
            if (foundOne && tokenLen > 0) {
                CompoundToken ct = new CompoundToken(len - tokenLen, tokenLen);
                tokens.add(ct);
            }
        }
    }
}