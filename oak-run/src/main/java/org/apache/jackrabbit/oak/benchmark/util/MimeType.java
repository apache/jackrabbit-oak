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
package org.apache.jackrabbit.oak.benchmark.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;

public enum MimeType {
    APPLICATION_DVI("application/x-dvi"),
    APPLICATION_FLASH("application/x-shockwave-flash"),
    APPLICATION_INDESIGN("application/x-indesign"),
    APPLICATION_JAVAARCHIVE("application/java-archive"),
    APPLICATION_MSEXCEL("application/vnd.ms-excel"),
    APPLICATION_MSPOWERPOINT("application/vnd.ms-powerpoint"),
    APPLICATION_MSWORD("application/msword"),
    APPLICATION_OD_GRAPHICS("application/vnd.oasis.opendocument.graphics"),
    APPLICATION_OD_PRESENTATION("application/vnd.oasis.opendocument.presentation"),
    APPLICATION_OD_SPREADSHEET("application/vnd.oasis.opendocument.spreadsheet"),
    APPLICATION_OD_TEXT("application/vnd.oasis.opendocument.text"),
    APPLICATION_OXML_DOCUMENT("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    APPLICATION_OXML_PRESENTATION("application/vnd.openxmlformats-officedocument.presentationml.presentation"),
    APPLICATION_OX_SPREADSHEET("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    APPLICATION_PACKAGE("application/vnd.adobe.air-application-installer-package+zip"),
    APPLICATION_PDF("application/pdf"),
    APPLICATION_POSTSCRIPT("application/postscript"),
    APPLICATION_QUARKXPRESS("application.vnd.quark.quarkxpress"),
    APPLICATION_RAR("application/x-rar-compressed"),
    APPLICATION_TAR("application/x-tar"),
    APPLICATION_TARGZ("application/x-tar-gz"),
    APPLICATION_ZIP("application/zip"),
    AUDIO_3GPP("audio/3gpp"),
    AUDIO_AAC("audio/x-aac"),
    AUDIO_MIDI("audio/midi"),
    AUDIO_MP3("audio/mp3"),
    AUDIO_MP4("audio/mp4"),
    AUDIO_MPEG("audio/mpeg"),
    AUDIO_OGG("audio/ogg"),
    AUDIO_REALAUDIO("audio/vnd.rn-realaudio"),
    AUDIO_VORBIS("audio/vorbis"),
    AUDIO_WAV("audio/x-wav"),
    AUDIO_WMA("audio/x-ms-wma"),
    IMAGE_BMP("image/x-bmp"),
    IMAGE_BPM("image/pbm"),
    IMAGE_DCRAW("image/x-dcraw"),
    IMAGE_DNG("image/x-adobe-dng"),
    IMAGE_GIF("image/gif"),
    IMAGE_ICON("image/vnd.microsoft.icon"),
    IMAGE_JPEG("image/jpeg"),
    IMAGE_PHOTOSHOP("image/vnd.adobe.photoshop"),
    IMAGE_PJPEG("image/pjpeg"),
    IMAGE_PNG("image/png"),
    IMAGE_PPM("image/x-ppm"),
    IMAGE_RAW_NIKON("image/x-raw-nikon"),
    IMAGE_SVG("image/svg+xml"),
    IMAGE_TIFF("image/tiff"),
    IMAGE_XCF("image/x-xcf"),
    IMAGE_XPBM("image/x-pbm"),
    TEXT_HTML("text/html"),
    TEXT_PLAIN("text/plain"),
    TEXT_RTF("text/rtf"),
    VIDEO_MP4("video/mp4"),
    VIDEO_MPEG("video/mpeg"),
    VIDEO_OGG("video/ogg"),
    VIDEO_QUICKTIME("video/quicktime"),
    VIDEO_WMV("video/x-ms-wmv"),
    VIDEO_XFLV("video/x-flv");
    
    private final String value;
    
    MimeType(@Nonnull final String value) {
        this.value = value;
    }
    
    /**
     * retrieve the string representation of the current Mime-Type
     * @return
     */
    public String getValue() {
        return value;
    }
    
    private static final List<MimeType> VALUES = Collections.unmodifiableList(Arrays
        .asList(values()));
    private static final int SIZE = VALUES.size();
    private static final Random RND = new Random();
    
    /**
     * retrieve a random Mime-Type from the available collection
     * @return
     */
    public static MimeType randomMimeType() {
        return VALUES.get(RND.nextInt(SIZE));
    }
}

