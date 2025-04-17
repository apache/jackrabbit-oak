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
package org.apache.jackrabbit.oak.commons;

// experimental location for prototyping namespace related support functions

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class NamespaceHelperPrototypeTest {

    // map with 'optimal' prefix mappings; hard-wired should be ok
    private static final Map<String, String> KNOWN_PREFIXES =
            Map.of("http://purl.org/dc/terms/", "dc",
                    "http://ns.adobe.com/exif/1.0/", "exif");

    // public API: suggest an available prefix for the provided namespace
    // (while considering pre-existing mappings)
    private static @NotNull String suggestPrefix(@NotNull String namespace,
                                                 @NotNull Function<String, String> lookupPrefix) {
        // try hard-wired map
        String known = KNOWN_PREFIXES.get(namespace);

        // lookup using supplied mapper as well
        String lookedUpNamespace = known != null ? lookupPrefix.apply(known) : null;

        // return hardwired prefix if unused or mapper has the prefix mapped to the same namespace
        if (known != null && (lookedUpNamespace == null || namespace.equals(lookedUpNamespace))) {
            return known;
        } else {
            return makeUpPrefix(namespace, lookupPrefix);
        }
    }

    // public API: suggest an available prefix for the provided namespace
    public static @NotNull String suggestPrefix(String namespace) {
        return suggestPrefix(namespace, n -> null);
    }

    // suggest an available prefix for the provided namespace, based on the characters
    // in the namespace name
    // (while considering pre-existing mappings)
    private static @NotNull String makeUpPrefix(@NotNull String namespace,
                                                @NotNull Function<String, String> lookupNamespace) {
        String prefix = namespace.toLowerCase(Locale.ENGLISH);

        // https://www.w3.org/guide/editor/namespaces.html
        if (prefix.startsWith("http://www.w3c.org/ns/")) {
            prefix = prefix.substring(22);
        }

        // strip scheme when http(s)
        if (prefix.startsWith("http://")) {
            prefix = prefix.substring(7);
        } else if (prefix.startsWith("https://")) {
            prefix = prefix.substring(8);
        }

        // strip common host name prefixes
        if (prefix.startsWith("www.")) {
            prefix = prefix.substring(4);
        } else if (prefix.startsWith("ns")) {
            prefix = prefix.substring(3);
        }

        // strip trailing slash
        if (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }

        // TODO: make sure it's really a valid JCR name prefix
        prefix = prefix.replace("/", "-");

        String lookedUpNamespace = lookupNamespace.apply(prefix);
        if (lookedUpNamespace == null || lookedUpNamespace.equals(namespace)) {
            return prefix;
        } else {
            return makeUpPrefixTrySha256(namespace, lookupNamespace);
        }
    }

    private static @Nullable String getSha256(String namespace) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(namespace.getBytes(StandardCharsets.UTF_8));
            return new BigInteger(1, bytes).toString(16);
        } catch (NoSuchAlgorithmException e) {
            // this really, really should not happen
            return null;
        }
    }

    // suggest an available prefix for the provided namespace, based on the base64
    // representation of the namespace name, and a final fallback to UUID based
    // (while considering pre-existing mappings)
    private static @NotNull String makeUpPrefixTrySha256(@NotNull String namespace,
                                                         @NotNull Function<String, String> lookupNamespace) {

        String sha = getSha256(namespace);
        if (sha != null) {
            for (int i = 6; i <= sha.length(); i++) {
                String prefix = "s-" + sha.substring(0, i);
                if (!namespace.equals(lookupNamespace.apply(prefix))) {
                    // prefix not in use; so we're ok
                    return prefix;
                }
            }
        }

        // fallback to UUID
        return makeUpPrefixByUUID(namespace, lookupNamespace);
    }

    // suggest an available prefix for the provided namespace, based on the base64
    // representation of the namespace name, and a final fallback to UUID based
    // (while considering pre-existing mappings)
    private static @NotNull String makeUpPrefixByUUID(@NotNull String namespace,
                                                      @NotNull Function<String, String> lookupNamespace) {
        String prefix;

        do {
            prefix = "u-" + UUID.randomUUID();
        } while (namespace.equals(lookupNamespace.apply(prefix)));

        return prefix;
    }

    @Test
    public void testSuggestPrefix() {

        // hardwired
        assertEquals("dc", suggestPrefix("http://purl.org/dc/terms/"));
        assertEquals("exif", suggestPrefix("http://ns.adobe.com/exif/1.0/"));

        // unknown namespace, just scheme and host name
        assertEquals("example.org", suggestPrefix("https://example.org"));
        assertEquals("example.org", suggestPrefix("http://example.org"));
        assertEquals("example.org", suggestPrefix("http://ns.example.org/"));
        assertEquals("example.org", suggestPrefix("https://www.example.org"));

        // W3C rules
        assertEquals("foobar", suggestPrefix("http://www.w3c.org/ns/foobar"));

        // unknown namespace, ... with path
        assertEquals("example.org-foo", suggestPrefix("https://www.example.org/foo"));
    }

    @Test
    public void testSuggestPrefixWithExistingPrefixes() {

        // prefix that would be suggested is already mapped to the provided namespace
        assertEquals("dc", suggestPrefix("http://purl.org/dc/terms/",
                Map.of("http://purl.org/dc/terms/", "dc")::get));

        // use of hardwired prefix will not work as that prefix is already mapped to a different namespace
        assertEquals("purl.org-dc-terms", suggestPrefix("http://purl.org/dc/terms/",
                Map.of("dc", "http://purl.org/dc/terms/somethingelse")::get));

        // lookup generated by simple extraction from namespace name will not work as prefix is already assigned
        // -> generate based on sha256 encoding
        assertEquals("s-1578eb", suggestPrefix("http://purl.org/dc/terms/",
                Map.of("dc", "http://purl.org/dc/terms/somethingelse",
                        "purl.org-dc-terms", "http://purl.org/dc/terms/somethingelse")::get));
    }
}