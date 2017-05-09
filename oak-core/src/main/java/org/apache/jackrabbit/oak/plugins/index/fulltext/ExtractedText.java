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

package org.apache.jackrabbit.oak.plugins.index.fulltext;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExtractedText {
    public enum ExtractionResult {
        /**
         * Indicates that text extraction was successful and some text
         * was extracted
         */
        SUCCESS,
        /**
         * Indicates that no text was extracted. This can happen if the
         * mimeType for the binary is part of exclusion list
         */
        EMPTY,
        /**
         * Indicates that text extraction resulted in an error.
         * The {@link ExtractedText#getExtractedText()} might contain
         * more details
         */
        ERROR
    }

    public static final ExtractedText ERROR = new ExtractedText(ExtractionResult.ERROR);

    public static final ExtractedText EMPTY = new ExtractedText(ExtractionResult.EMPTY, "");

    private final ExtractionResult extractionResult;
    private final CharSequence extractedText;

    public ExtractedText(@Nonnull ExtractionResult extractionResult){
        this(extractionResult, null);
    }

    public ExtractedText(@Nonnull ExtractionResult extractionResult,CharSequence extractedText) {
        this.extractionResult = extractionResult;
        this.extractedText = extractedText;
        checkState();
    }

    @Nonnull
    public ExtractionResult getExtractionResult() {
        return extractionResult;
    }

    @CheckForNull
    public CharSequence getExtractedText() {
        return extractedText;
    }

    private void checkState() {
        if (extractionResult == ExtractionResult.SUCCESS){
            checkNotNull(extractedText, "extractedText must not be null for SUCCESS");
        }
    }
}
