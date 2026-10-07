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
package org.apache.jackrabbit.oak.plugins.index.diff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class DiffIndexMergerTest {

    public static DiffIndexMerger getMerger() {
        DiffIndexMerger merger = new DiffIndexMerger().
                setDeleteCopiesOutOfTheBoxIndex(true).
                setDeleteCreatesDummyIndex(true).
                setLogAtInfoLevel(true).
                setUnsupportedIncludedPaths(new String[] {"/apps", "/libs"});
        return merger;
    }

    @Test
    public void noCustomization() {
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-12": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                    }
                }
                """, true);

        getMerger().merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        assertEquals("{}", newImageLuceneDefinitions.toString());
    }

    // an index was customized in the past, but not any longer
    @Test
    public void noCustomizationAnyLonger() {
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-12": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "includedPaths": "/content/dam",
                        "tags": ["abc"],
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/damAssetLucene-12-custom-1": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "includedPaths": "/content/dam",
                        "mergeInfo": "This index was auto-merged. See also https://thomasmueller.github.io/oakTools/simplified.html",
                        "mergeChecksum": "ef0312b8bf7cc97bb8efd0faeeb4e4b1094268694a2df749b73fac08987b3264",
                        "merges": ["/oak:index/damAssetLucene"],
                        "indexRules": {
                            "jcr:primaryType": "nam:nt:unstructured",
                            "dam:Asset": {
                                "jcr:primaryType": "nam:nt:unstructured",
                                "properties": {
                                    "jcr:primaryType": "nam:nt:unstructured",
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true,
                                        "jcr:primaryType": "nam:nt:unstructured"
                                    },
                                    "y": {
                                        "name": "y",
                                        "propertyIndex": true,
                                        "jcr:primaryType": "nam:nt:unstructured"
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                    }
                }
                """, true);
        // we can not just move back to /oak:index/damAssetLucene-12, because
        // that index has a lower version and so is then not use for queries.
        // we need to explicitly create a new version.
        // once damAssetLucene-13 is rolled out, then no "-custom-" is created however
        getMerger().merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        assertEquals("""
                {
                  "/oak:index/damAssetLucene-12-custom-2": {
                    "jcr:primaryType": "oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "includedPaths": "/content/dam",
                    "tags": ["abc"],
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "aa02b93e0ba4ff4bcffb8247d38c5cc039be5afa915f822e18df7a578fabff76",
                    "merges": ["/oak:index/damAssetLucene"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "x": {
                            "name": "x",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", newImageLuceneDefinitions.toString());
    }

    @Test
    public void firstCustomization() {
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        JsonObject indexDiff = JsonObject.fromJson("""
                {
                    "damAssetLucene": {
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "y": {
                                        "name": "y",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    }
                }
            """, true);
        String indexDiffString = indexDiff.toString();
        String base64Prop =
                "\":blobId:" + Base64.getEncoder().encodeToString(indexDiffString.getBytes(StandardCharsets.UTF_8)) + "\"";
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-12": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "includedPaths": "/content/dam",
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "diff.json": {
                            "jcr:primaryType": "nam:nt:file",
                            "jcr:content": {
                                "jcr:primaryType": "nam:nt:resource",
                                "jcr:mimeType": "application/json",
                                "jcr:data":
                """ + base64Prop + """
                            }
                        }
                    }
                }
                """, true);
        getMerger().merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        assertEquals("""
                {
                  "/oak:index/damAssetLucene-12-custom-1": {
                    "jcr:primaryType": "oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "tags": ["abc"],
                    "includedPaths": "/content/dam",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "ef0312b8bf7cc97bb8efd0faeeb4e4b1094268694a2df749b73fac08987b3264",
                    "merges": ["/oak:index/damAssetLucene"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "x": {
                            "name": "x",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          },
                          "y": {
                            "name": "y",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", newImageLuceneDefinitions.toString());
    }

    private static String jsonFileNode(String json) {
        return """
                {
                    "jcr:primaryType": "nam:nt:file",
                    "jcr:content": {
                        "jcr:primaryType": "nam:nt:resource",
                        "jcr:mimeType": "application/json",
                        "jcr:data": \"""" + ":blobId:"
                + Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)) + """
                \"
                    }
                }
                """;
    }

    // multiple *.json files are read, sorted by name, and their top-level entries
    // are merged; the alphabetically higher file overwrites the earlier entries
    @Test
    public void multipleDiffJsonFiles() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "c.json": """ + jsonFileNode("{\"a\": {\"y\": \"2\"}}") + """
                        , "a.json": """ + jsonFileNode("{\"a\": {\"x\": \"1\"}}") + """
                        , "b.json": """ + jsonFileNode("{\"b\": {\"z\": \"3\"}}") + """
                        , "readme.txt": { "jcr:primaryType": "nam:nt:unstructured" }
                    }
                }
                """, true);

        HashMap<String, JsonObject> target = new HashMap<>();
        assertNull(getMerger().tryExtractDiffIndex(repositoryDefinitions, "/oak:index/diff.index", target));
        assertEquals("[a, b]", new TreeSet<>(target.keySet()).toString());
        // "a" is not deep-merged: the entry of "c.json" replaces the one of "a.json"
        assertEquals("{\n  \"y\": \"2\"\n}", target.get("a").toString());
        assertEquals("{\n  \"z\": \"3\"\n}", target.get("b").toString());
    }

    // a binary within the diff JSON may reference another file of the diff.index,
    // instead of inlining the base64 encoded data
    @Test
    public void diffJsonWithFileReference() {
        String helloWorld = ":blobId:" + Base64.getEncoder().encodeToString(
                "Hello World".getBytes(StandardCharsets.UTF_8));
        String diffWithFileRef = """
                {
                  "damAssetLucene": {
                    "test.txt": {
                      "jcr:primaryType": "nam:nt:file",
                      "jcr:content": {
                        "jcr:primaryType": "nam:nt:resource",
                        "jcr:mimeType": "text/plain",
                        "jcr:data": ":file:test.txt"
                      }
                    }
                  }
                }
                """;
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "test.txt": {
                            "jcr:primaryType": "nam:nt:file",
                            "jcr:content": {
                                "jcr:primaryType": "nam:nt:resource",
                                "jcr:mimeType": "text/plain",
                                "jcr:data": \"""" + helloWorld + """
                \"
                            }
                        },
                        "diff.json": """ + jsonFileNode(diffWithFileRef) + """
                    }
                }
                """, true);

        HashMap<String, JsonObject> target = new HashMap<>();
        assertNull(getMerger().tryExtractDiffIndex(repositoryDefinitions, "/oak:index/diff.index", target));
        JsonObject jcrContent = target.get("damAssetLucene").
                getChildren().get("test.txt").getChildren().get("jcr:content");
        // the reference is replaced with the binary value of the referenced file
        assertEquals("Hello World", JsonNodeUpdater.oakStringValue(jcrContent, "jcr:data"));
    }

    // the same content, but with the base64 data inlined in the diff JSON,
    // results in exactly the same diff
    @Test
    public void diffJsonWithInlinedBinary() {
        String helloWorld = ":blobId:" + Base64.getEncoder().encodeToString(
                "Hello World".getBytes(StandardCharsets.UTF_8));
        String diffWithInlinedBinary = """
                {
                  "damAssetLucene": {
                    "test.txt": {
                      "jcr:primaryType": "nam:nt:file",
                      "jcr:content": {
                        "jcr:primaryType": "nam:nt:resource",
                        "jcr:mimeType": "text/plain",
                        "jcr:data": \"""" + helloWorld + """
                \"
                      }
                    }
                  }
                }
                """;
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "diff.json": """ + jsonFileNode(diffWithInlinedBinary) + """
                    }
                }
                """, true);

        HashMap<String, JsonObject> target = new HashMap<>();
        assertNull(getMerger().tryExtractDiffIndex(repositoryDefinitions, "/oak:index/diff.index", target));
        JsonObject jcrContent = target.get("damAssetLucene").
                getChildren().get("test.txt").getChildren().get("jcr:content");
        assertEquals("Hello World", JsonNodeUpdater.oakStringValue(jcrContent, "jcr:data"));
    }

    // a file reference within the nested "diff" JSON object is resolved as well
    @Test
    public void nestedDiffWithFileReference() {
        String helloWorld = ":blobId:" + Base64.getEncoder().encodeToString(
                "Hello World".getBytes(StandardCharsets.UTF_8));
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "test.txt": {
                            "jcr:primaryType": "nam:nt:file",
                            "jcr:content": {
                                "jcr:primaryType": "nam:nt:resource",
                                "jcr:mimeType": "text/plain",
                                "jcr:data": \"""" + helloWorld + """
                \"
                            }
                        },
                        "diff": {
                            "damAssetLucene": {
                                "test.txt": {
                                    "jcr:primaryType": "nam:nt:file",
                                    "jcr:content": {
                                        "jcr:primaryType": "nam:nt:resource",
                                        "jcr:mimeType": "text/plain",
                                        "jcr:data": ":file:test.txt"
                                    }
                                }
                            }
                        }
                    }
                }
                """, true);

        HashMap<String, JsonObject> target = new HashMap<>();
        assertNull(getMerger().tryExtractDiffIndex(repositoryDefinitions, "/oak:index/diff.index", target));
        JsonObject jcrContent = target.get("damAssetLucene").
                getChildren().get("test.txt").getChildren().get("jcr:content");
        assertEquals("Hello World", JsonNodeUpdater.oakStringValue(jcrContent, "jcr:data"));
        // the input is not modified
        assertEquals("\":file:test.txt\"", repositoryDefinitions.
                getChildren().get("/oak:index/diff.index").
                getChildren().get("diff").
                getChildren().get("damAssetLucene").
                getChildren().get("test.txt").
                getChildren().get("jcr:content").
                getProperties().get("jcr:data"));
    }

    // the diff.index that is stored in the result keeps the file references;
    // only the generated index definition contains the resolved binary
    @Test
    public void fileReferenceIsKeptInStoredDiffIndex() {
        String helloWorld = ":blobId:" + Base64.getEncoder().encodeToString(
                "Hello World".getBytes(StandardCharsets.UTF_8));
        String diffWithFileRef = """
                {
                  "damAssetLucene": {
                    "test.txt": {
                      "jcr:primaryType": "nam:nt:file",
                      "jcr:content": {
                        "jcr:primaryType": "nam:nt:resource",
                        "jcr:mimeType": "text/plain",
                        "jcr:data": ":file:test.txt"
                      }
                    }
                  }
                }
                """;
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "test.txt": {
                            "jcr:primaryType": "nam:nt:file",
                            "jcr:content": {
                                "jcr:primaryType": "nam:nt:resource",
                                "jcr:mimeType": "text/plain",
                                "jcr:data": \"""" + helloWorld + """
                \"
                            }
                        },
                        "diff.json": """ + jsonFileNode(diffWithFileRef) + """
                    }
                }
                """, true);
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-12": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "includedPaths": "/content/dam"
                    }
                }
                """, true);

        getMerger().merge(newImageLuceneDefinitions, repositoryDefinitions, null);

        // the generated index definition contains the resolved binary
        JsonObject merged = newImageLuceneDefinitions.getChildren().get("/oak:index/damAssetLucene-12-custom-1");
        JsonObject mergedContent = merged.getChildren().get("test.txt").getChildren().get("jcr:content");
        assertEquals("Hello World", JsonNodeUpdater.oakStringValue(mergedContent, "jcr:data"));

        // the diff.index that is stored in the result keeps the unresolved reference
        JsonObject storedDiffIndex = newImageLuceneDefinitions.getChildren().get("/oak:index/diff.index");
        String storedDiff = JsonNodeUpdater.oakStringValue(
                storedDiffIndex.getChildren().get("diff.json").getChildren().get("jcr:content"), "jcr:data");
        assertEquals(diffWithFileRef, storedDiff);
    }

    // All *.json children are sorted alphabetically, and merged.
    // For duplicate indexes, the (alphabetically) last one wins.
    @Test
    public void openListOfDiffs() {
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "diff.json": """ + jsonFileNode(indexDiff("ntFileFolder", "a", "1")) + """
                        , "diff.asset1.json": """ + jsonFileNode(indexDiff("damAssetLucene", "b", "2")) + """
                        , "diff.asset2.json": """ + jsonFileNode(indexDiff("damAssetLucene", "b", "3")) + """
                    }
                }
                """, true);
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-12": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "includedPaths": "/content/dam"
                    },
                    "/oak:index/ntFileFolder-3": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "includedPaths": "/content/dam"
                    }
                }
                """, true);

        getMerger().merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        assertEquals("""
                {
                  "/oak:index/diff.index": {
                    "jcr:primaryType": "nt:unstructured",
                    "type": "lucene",
                    "includedPaths": "/same",
                    "queryPaths": "/same",
                    "diff.json": {
                      "jcr:primaryType": "nam:nt:file",
                      "jcr:content": {
                        "jcr:primaryType": "nam:nt:resource",
                        "jcr:mimeType": "application/json",
                        "jcr:data": ":blobId:ewogICJudEZpbGVGb2xkZXIiOiB7ICJpbmRleFJ1bGVzIjogewogICAgImRhbTpBc3NldCI6IHsKICAgICAgInByb3BlcnRpZXMiOiB7CiAgICAgICAgImEiOiB7CiAgICAgICAgICAgICJuYW1lIjogIjEiLAogICAgICAgICAgICAicHJvcGVydHlJbmRleCI6IHRydWUKICAgICAgICAgIH0KICAgICAgICB9CiAgICAgIH0KICAgIH0KICB9Cn0K"
                      }
                    },
                    "diff.asset1.json": {
                      "jcr:primaryType": "nam:nt:file",
                      "jcr:content": {
                        "jcr:primaryType": "nam:nt:resource",
                        "jcr:mimeType": "application/json",
                        "jcr:data": ":blobId:ewogICJkYW1Bc3NldEx1Y2VuZSI6IHsgImluZGV4UnVsZXMiOiB7CiAgICAiZGFtOkFzc2V0IjogewogICAgICAicHJvcGVydGllcyI6IHsKICAgICAgICAiYiI6IHsKICAgICAgICAgICAgIm5hbWUiOiAiMiIsCiAgICAgICAgICAgICJwcm9wZXJ0eUluZGV4IjogdHJ1ZQogICAgICAgICAgfQogICAgICAgIH0KICAgICAgfQogICAgfQogIH0KfQo="
                      }
                    },
                    "diff.asset2.json": {
                      "jcr:primaryType": "nam:nt:file",
                      "jcr:content": {
                        "jcr:primaryType": "nam:nt:resource",
                        "jcr:mimeType": "application/json",
                        "jcr:data": ":blobId:ewogICJkYW1Bc3NldEx1Y2VuZSI6IHsgImluZGV4UnVsZXMiOiB7CiAgICAiZGFtOkFzc2V0IjogewogICAgICAicHJvcGVydGllcyI6IHsKICAgICAgICAiYiI6IHsKICAgICAgICAgICAgIm5hbWUiOiAiMyIsCiAgICAgICAgICAgICJwcm9wZXJ0eUluZGV4IjogdHJ1ZQogICAgICAgICAgfQogICAgICAgIH0KICAgICAgfQogICAgfQogIH0KfQo="
                      }
                    }
                  },
                  "/oak:index/ntFileFolder-3-custom-1": {
                    "jcr:primaryType": "oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "includedPaths": "/content/dam",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "a7ac2c59503b5bfea38c78a3299a536965a79f744eeabbd060381caa6e47037c",
                    "merges": ["/oak:index/ntFileFolder"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "a": {
                            "name": "1",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  },
                  "/oak:index/damAssetLucene-12-custom-1": {
                    "jcr:primaryType": "oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "includedPaths": "/content/dam",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "0cf99580627a63f488212a5f54f8f762f7e7a51af41181ee019ef9a9f58b0d9a",
                    "merges": ["/oak:index/damAssetLucene"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "b": {
                            "name": "3",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", newImageLuceneDefinitions.toString());
    }

    private static String indexDiff(String indexName, String propertyName, String value) {
        return """
                {
                  \"""" + indexName + """
                  \": { "indexRules": {
                      "dam:Asset": {
                        "properties": {
                          \"""" + propertyName + """
                \": {
                            "name": \"""" + value + """
                \",
                            "propertyIndex": true
                          }
                        }
                      }
                    }
                  }
                }
                """;
    }

    // referencing a file that doesn't exist results in an error message
    @Test
    public void diffJsonWithMissingFileReference() {
        String diffWithFileRef = """
                {
                  "damAssetLucene": {
                    "test.txt": {
                      "jcr:primaryType": "nam:nt:file",
                      "jcr:content": {
                        "jcr:primaryType": "nam:nt:resource",
                        "jcr:data": ":file:test.txt"
                      }
                    }
                  }
                }
                """;
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "diff.json": """ + jsonFileNode(diffWithFileRef) + """
                    }
                }
                """, true);

        HashMap<String, JsonObject> target = new HashMap<>();
        assertEquals("Referenced file is missing: test.txt",
                getMerger().tryExtractDiffIndex(repositoryDefinitions, "/oak:index/diff.index", target));
        assertEquals("[]", target.keySet().toString());
    }

    @Test
    public void inlinedBinaryIsNotInterpretedAsFileReference() {
        String binary = ":blobId:" + Base64.getEncoder().encodeToString(
                ":file:missing.txt".getBytes(StandardCharsets.UTF_8));
        JsonObject definitions = JsonObject.fromJson(
                "{\"/oak:index/diff.index\":{\"diff\":{\"acme.test\":{\"jcr:data\":\"" + binary + "\"}}}}", true);
        HashMap<String, JsonObject> target = new HashMap<>();
        assertNull(getMerger().tryExtractDiffIndex(definitions, "/oak:index/diff.index", target));
        assertEquals("\"" + binary + "\"", target.get("acme.test").getProperties().get("jcr:data"));
    }

    @Test
    public void invalidDiffDoesNotDisablePreviouslyMergedIndexes() {
        JsonObject repository = JsonObject.fromJson("""
                {
                    "/oak:index/acme.test-1-custom-1": {
                        "type": "lucene",
                        "mergeInfo": "previously merged",
                        "mergeChecksum": "old"
                    }
                }
                """, true);
        for (String diff : new String[] {
                "{broken",
                "{\"acme.test\":{\"file\":{\"jcr:data\":\":file:missing.txt\"}}}" }) {
            JsonObject definitions = JsonObject.fromJson(
                    "{\"/oak:index/diff.index\":{\"diff.json\":" + jsonFileNode(diff) + "}}", true);
            String original = definitions.toString();
            DiffIndexMerger merger = getMerger();
            merger.merge(definitions, repository, null);
            assertEquals(original, definitions.toString());
            assertEquals(1, merger.getAndClearWarnings().size());
        }
    }

    @Test
    public void invalidOptimizerDoesNotApplyCustomerChanges() {
        JsonObject definitions = JsonObject.fromJson(
                "{\"/oak:index/diff.index\":{\"diff.json\":"
                        + jsonFileNode("{\"acme.test\":{\"type\":\"lucene\"}}")
                        + "},\"/oak:index/diff.index.optimizer\":{\"diff.json\":"
                        + jsonFileNode("{broken") + "}}", true);
        String original = definitions.toString();
        getMerger().merge(definitions, new JsonObject(true), null);
        assertEquals(original, definitions.toString());
    }

    @Test
    public void firstCustomizationAsJson() {
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        // notice the the diff.index.optimizer is already stored in the repo,
        // and so does not need to appear in the newImageLuceneDefinitions
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-12": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "includedPaths": "/content/dam",
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/diff.index.optimizer": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "diff": {
                           "damAssetLucene": {
                                "indexRules": {
                                    "dam:Asset": {
                                        "properties": {
                                            "y": {
                                                "name": "y",
                                                "propertyIndex": true
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                """, true);

        getMerger().merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        assertEquals("""
                {
                  "/oak:index/damAssetLucene-12-custom-1": {
                    "jcr:primaryType": "oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "tags": ["abc"],
                    "includedPaths": "/content/dam",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "ef0312b8bf7cc97bb8efd0faeeb4e4b1094268694a2df749b73fac08987b3264",
                    "merges": ["/oak:index/damAssetLucene"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "x": {
                            "name": "x",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          },
                          "y": {
                            "name": "y",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", newImageLuceneDefinitions.toString());
    }

    @Test
    public void newIndexAndNewCustomization() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-11": {
                        "jcr:primaryType": "nam:oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "includedPaths": "/content/dam",
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "a": {
                                        "name": "a",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    }
                }
                """, true);
        // notice the the diff.index is not yet stored in the repo,
        // and so _does_ need to appear in the newImageLuceneDefinitions
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-12": {
                        "jcr:primaryType": "nam:oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "includedPaths": "/content/dam",
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "diff": {
                           "damAssetLucene": {
                                "indexRules": {
                                    "dam:Asset": {
                                        "properties": {
                                            "y": {
                                                "name": "y",
                                                "propertyIndex": true
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            """, true);

        getMerger().merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        assertEquals("""
                {
                  "/oak:index/damAssetLucene-12": {
                    "jcr:primaryType": "nam:oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "tags": ["abc"],
                    "includedPaths": "/content/dam",
                    "indexRules": {
                      "dam:Asset": {
                        "properties": {
                          "x": {
                            "name": "x",
                            "propertyIndex": true
                          }
                        }
                      }
                    }
                  },
                  "/oak:index/diff.index": {
                    "jcr:primaryType": "nam:nt:unstructured",
                    "type": "lucene",
                    "includedPaths": "/same",
                    "queryPaths": "/same",
                    "diff": {
                      "damAssetLucene": {
                        "indexRules": {
                          "dam:Asset": {
                            "properties": {
                              "y": {
                                "name": "y",
                                "propertyIndex": true
                              }
                            }
                          }
                        }
                      }
                    }
                  },
                  "/oak:index/damAssetLucene-12-custom-1": {
                    "jcr:primaryType": "nam:oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "tags": ["abc"],
                    "includedPaths": "/content/dam",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "ef0312b8bf7cc97bb8efd0faeeb4e4b1094268694a2df749b73fac08987b3264",
                    "merges": ["/oak:index/damAssetLucene"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "x": {
                            "name": "x",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          },
                          "y": {
                            "name": "y",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", newImageLuceneDefinitions.toString());
    }

    @Test
    public void mergeBothCustomAndAdobeProvidedDiff() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-12": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "includedPaths": "/content/dam",
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/diff.index.optimizer": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene",
                        "includedPaths": "/same",
                        "queryPaths": "/same",
                        "diff": {
                            "damAssetLucene": {
                                "indexRules": {
                                    "dam:Asset": {
                                        "properties": {
                                            "y": {
                                                "name": "y",
                                                "propertyIndex": true
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                """, true);
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene",
                        "includedPaths": "/same",
                        "queryPaths": "/same",
                        "diff": {
                            "damAssetLucene": {
                                "indexRules": {
                                    "dam:Asset": {
                                        "properties": {
                                            "z": {
                                                "name": "z",
                                                "propertyIndex": true
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            """, true);
        getMerger().merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        assertEquals("""
                {
                  "/oak:index/diff.index": {
                    "jcr:primaryType": "nt:unstructured",
                    "type": "lucene",
                    "includedPaths": "/same",
                    "queryPaths": "/same",
                    "diff": {
                      "damAssetLucene": {
                        "indexRules": {
                          "dam:Asset": {
                            "properties": {
                              "z": {
                                "name": "z",
                                "propertyIndex": true
                              }
                            }
                          }
                        }
                      }
                    }
                  },
                  "/oak:index/damAssetLucene-12-custom-1": {
                    "jcr:primaryType": "oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "tags": ["abc"],
                    "includedPaths": "/content/dam",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "9957cea378122a0b88216de81274b13eed3746e953b91340c51bd322dea2e353",
                    "merges": ["/oak:index/damAssetLucene"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "x": {
                            "name": "x",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          },
                          "z": {
                            "name": "z",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          },
                          "y": {
                            "name": "y",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", newImageLuceneDefinitions.toString());
    }

    @Test
    public void similarName() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-12": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "includedPaths": "/content/dam",
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/damAsset-12": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "includedPaths": "/content/dam",
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    }
                }
                """, true);
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene",
                        "includedPaths": "/same",
                        "queryPaths": "/same",
                        "diff": {
                            "damAsset": {
                                "indexRules": {
                                    "dam:Asset": {
                                        "properties": {
                                            "z": {
                                                "name": "z",
                                                "propertyIndex": true
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            """, true);
        getMerger().merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        assertEquals("""
                {
                  "/oak:index/diff.index": {
                    "jcr:primaryType": "nt:unstructured",
                    "type": "lucene",
                    "includedPaths": "/same",
                    "queryPaths": "/same",
                    "diff": {
                      "damAsset": {
                        "indexRules": {
                          "dam:Asset": {
                            "properties": {
                              "z": {
                                "name": "z",
                                "propertyIndex": true
                              }
                            }
                          }
                        }
                      }
                    }
                  },
                  "/oak:index/damAsset-12-custom-1": {
                    "jcr:primaryType": "oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "tags": ["abc"],
                    "includedPaths": "/content/dam",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "09d85a55873fbf763bbf122bbd61198ea03e4ddd658d24a9dc0c0ac48ff1c977",
                    "merges": ["/oak:index/damAsset"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "x": {
                            "name": "x",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          },
                          "z": {
                            "name": "z",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", newImageLuceneDefinitions.toString());
    }

    private static NodeStore storeInRepository(String path, String json) throws CommitFailedException, IOException {
        NodeStore ns = new MemoryNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        JsonNodeUpdater.addOrReplace(builder, ns, path, "nt:unstructured", json);
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return ns;
    }

    @Test
    public void removeUnchangedDiffIndex() throws Exception {
        NodeStore repositoryNodeStore = storeInRepository("/oak:index/diff.index", """
                {
                    "jcr:primaryType": "nt:unstructured",
                    "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                }""");

        // same content, just properties in a different order: should still be considered unchanged
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "queryPaths": "/a", "includedPaths": "/a", "type": "lucene"
                    }
                }
                """, true);

        getMerger().removeUnchangedDiffIndexEntries(newImageLuceneDefinitions, repositoryNodeStore);
        assertEquals("{}", newImageLuceneDefinitions.toString());
    }

    @Test
    public void keepChangedDiffIndex() throws Exception {
        NodeStore repositoryNodeStore = storeInRepository("/oak:index/diff.index", """
                {
                    "jcr:primaryType": "nt:unstructured",
                    "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                }""");

        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/b", "queryPaths": "/b"
                    }
                }
                """, true);

        getMerger().removeUnchangedDiffIndexEntries(newImageLuceneDefinitions, repositoryNodeStore);
        assertEquals(1, newImageLuceneDefinitions.getChildren().size());
    }

    @Test
    public void keepDiffIndexNotYetInRepository() throws Exception {
        NodeStore repositoryNodeStore = new MemoryNodeStore();

        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                    }
                }
                """, true);

        getMerger().removeUnchangedDiffIndexEntries(newImageLuceneDefinitions, repositoryNodeStore);
        assertEquals(1, newImageLuceneDefinitions.getChildren().size());
    }

    @Test
    public void keepDiffIndexWhenRepositoryNodeStoreIsNull() {
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                    }
                }
                """, true);

        getMerger().removeUnchangedDiffIndexEntries(newImageLuceneDefinitions, null);
        assertEquals(1, newImageLuceneDefinitions.getChildren().size());
    }

    @Test
    public void diffIndexAndDiffIndexOptimizerAreIndependent() throws Exception {
        NodeStore repositoryNodeStore = storeInRepository("/oak:index/diff.index", """
                {
                    "jcr:primaryType": "nt:unstructured",
                    "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                }""");
        NodeBuilder builder = repositoryNodeStore.getRoot().builder();
        JsonNodeUpdater.addOrReplace(builder, repositoryNodeStore, "/oak:index/diff.index.optimizer", "nt:unstructured", """
                {
                    "jcr:primaryType": "nt:unstructured",
                    "type": "lucene", "includedPaths": "/x", "queryPaths": "/x"
                }""");
        repositoryNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                    },
                    "/oak:index/diff.index.optimizer": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/y", "queryPaths": "/y"
                    }
                }
                """, true);

        getMerger().removeUnchangedDiffIndexEntries(newImageLuceneDefinitions, repositoryNodeStore);
        // diff.index is unchanged (removed), diff.index.optimizer changed (kept)
        assertEquals("[/oak:index/diff.index.optimizer]", newImageLuceneDefinitions.getChildren().keySet().toString());
    }

    @Test
    public void removeDiffIndexEntriesRemovesBothUnconditionally() {
        JsonObject definitions = JsonObject.fromJson("""
                {
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                    },
                    "/oak:index/diff.index.optimizer": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/b", "queryPaths": "/b"
                    },
                    "/oak:index/damAssetLucene-8": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "elasticsearch"
                    }
                }
                """, true);

        DiffIndexMerger.removeDiffIndexEntries(definitions);
        assertEquals("[/oak:index/damAssetLucene-8]", definitions.getChildren().keySet().toString());
    }

    // when migrating from the old way to configure indexes
    // to simplified index management, a new version of the index
    // needs to be created _even if the definition exactly matches_
    // because the old configuration might be removed.
    // the flag to check is "mergeChecksum"
    @Test
    public void migrateFromCustomized() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/acme.test-1-custom-1": {
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "includedPaths": "/content/dam",
                        "queryPaths": "/content/dam",
                        "jcr:primaryType": "nam:oak:QueryIndexDefinition",
                        "indexRules": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "dam:Asset": {
                            "jcr:primaryType": "nam:nt:unstructured",
                            "properties": {
                              "jcr:primaryType": "nam:nt:unstructured",
                              "x": {
                                "name": "x",
                                "propertyIndex": true,
                                "jcr:primaryType": "nam:nt:unstructured"
                              }
                            }
                          }
                        }
                    },
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a",
                        "diff": {
                            "acme.test": {
                                "type": "lucene",
                                "async": ["async", "nrt"],
                                "includedPaths": "/content/dam",
                                "queryPaths": "/content/dam",
                                "jcr:primaryType": "nam:oak:QueryIndexDefinition",
                                "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                                "mergeChecksum": "cfa1e6e503fdb74a9fb87bbbc46603bb06fb8caa426abb192ded47276c763404",
                                "merges": ["/oak:index/acme.test"],
                                "indexRules": {
                                  "jcr:primaryType": "nam:nt:unstructured",
                                  "dam:Asset": {
                                    "jcr:primaryType": "nam:nt:unstructured",
                                    "properties": {
                                      "jcr:primaryType": "nam:nt:unstructured",
                                      "x": {
                                        "name": "x",
                                        "propertyIndex": true,
                                        "jcr:primaryType": "nam:nt:unstructured"
                                      }
                                    }
                                  }
                                }
                              }
                        }
                    }
                }
                """, true);
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        DiffIndexMerger merger = new DiffIndexMerger().
                setDeleteCopiesOutOfTheBoxIndex(false).
                setDeleteCreatesDummyIndex(true).
                setLogAtInfoLevel(true);
        merger.merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        JsonObject merged = newImageLuceneDefinitions;
        assertEquals("""
                {
                  "/oak:index/acme.test-1-custom-2": {
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "includedPaths": "/content/dam",
                    "queryPaths": "/content/dam",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "cfa1e6e503fdb74a9fb87bbbc46603bb06fb8caa426abb192ded47276c763404",
                    "merges": ["/oak:index/acme.test"],
                    "jcr:primaryType": "nam:oak:QueryIndexDefinition",
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "x": {
                            "name": "x",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", merged.toString());
    }

    // GRANITE-68995
    // Support indexes without version, eg. commerceLucene
    @Test
    public void unversionedMerge() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/commerceLucene": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "tags": ["abc"],
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/same", "queryPaths": "/same",
                        "diff": {
                           "commerceLucene": {
                                "indexRules": {
                                    "dam:Asset": {
                                        "properties": {
                                            "y": {
                                                "name": "y",
                                                "propertyIndex": true
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                """, true);
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        DiffIndexMerger merger = new DiffIndexMerger().
                setDeleteCopiesOutOfTheBoxIndex(false).
                setDeleteCreatesDummyIndex(true).
                setLogAtInfoLevel(true);
        merger.merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        JsonObject merged = newImageLuceneDefinitions;
        assertEquals("""
                {
                  "/oak:index/commerceLucene-custom-1": {
                    "jcr:primaryType": "oak:IndexDefinition",
                    "type": "lucene",
                    "async": ["async", "nrt"],
                    "tags": ["abc"],
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "138dd3bf22232a733b868b8d463a196aaf708993f06008a08c9e8813c4acaa65",
                    "merges": ["/oak:index/commerceLucene"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "x": {
                            "name": "x",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          },
                          "y": {
                            "name": "y",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", merged.toString());
    }

    // if there is a matching index with a "mergeChecksum",
    // that matches exactly with the current diff.index configuration
    // then no new index needs to be created (the checksum itself is ignored)
    @Test
    public void withExistingMergeChecksum() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/acme.test-1-custom-1": {
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "includedPaths": "/content/dam",
                        "queryPaths": "/content/dam",
                        "mergeChecksum": "test",
                        "jcr:primaryType": "nam:oak:QueryIndexDefinition",
                        "indexRules": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "dam:Asset": {
                            "jcr:primaryType": "nam:nt:unstructured",
                            "properties": {
                              "jcr:primaryType": "nam:nt:unstructured",
                              "x": {
                                "name": "x",
                                "propertyIndex": true,
                                "jcr:primaryType": "nam:nt:unstructured"
                              }
                            }
                          }
                        }
                    },
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a",
                        "diff": {
                            "acme.test": {
                                "type": "lucene",
                                "async": ["async", "nrt"],
                                "includedPaths": "/content/dam",
                                "queryPaths": "/content/dam",
                                "jcr:primaryType": "nam:oak:QueryIndexDefinition",
                                "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                                "mergeChecksum": "cfa1e6e503fdb74a9fb87bbbc46603bb06fb8caa426abb192ded47276c763404",
                                "merges": ["/oak:index/acme.test"],
                                "indexRules": {
                                  "jcr:primaryType": "nam:nt:unstructured",
                                  "dam:Asset": {
                                    "jcr:primaryType": "nam:nt:unstructured",
                                    "properties": {
                                      "jcr:primaryType": "nam:nt:unstructured",
                                      "x": {
                                        "name": "x",
                                        "propertyIndex": true,
                                        "jcr:primaryType": "nam:nt:unstructured"
                                      }
                                    }
                                  }
                                }
                              }
                        }
                    }
                }
                """, true);
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        DiffIndexMerger merger = new DiffIndexMerger().
                setDeleteCopiesOutOfTheBoxIndex(false).
                setDeleteCreatesDummyIndex(true).
                setLogAtInfoLevel(true);
        merger.merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        JsonObject merged = newImageLuceneDefinitions;
        assertEquals("""
                {}""", merged.toString());
    }

    // when migrating from simplified index management
    // to the legacy mode (for example, in an emergency case,
    // or when the index requires apps + libs)
    // then we need to ignore disabled indexes that have the mergeInfo
    // property
    @Test
    public void migrateBackToLegacyMode() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/acme.test-1-custom-1": {
                        "type": "disabled",
                        "mergeInfo": "This index was created with simplified index management",
                        "async": ["async", "nrt"],
                        "includedPaths": "/content/dam",
                        "queryPaths": "/content/dam",
                        "jcr:primaryType": "nam:oak:QueryIndexDefinition",
                        "indexRules": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "dam:Asset": {
                            "jcr:primaryType": "nam:nt:unstructured",
                            "properties": {
                              "jcr:primaryType": "nam:nt:unstructured",
                              "x": {
                                "name": "x",
                                "propertyIndex": true,
                                "jcr:primaryType": "nam:nt:unstructured"
                              }
                            }
                          }
                        }
                    },
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a",
                        "diff": {
                        }
                    }
                }
                """, true);
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        DiffIndexMerger merger = new DiffIndexMerger().
                setDeleteCopiesOutOfTheBoxIndex(false).
                setDeleteCreatesDummyIndex(true).
                setLogAtInfoLevel(true);
        merger.merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        JsonObject merged = newImageLuceneDefinitions;
        assertEquals("{}", merged.toString());
    }

    // GRANITE-69761: when a fully custom index is removed from diff.index, and
    // deleteCreatesDummyIndex is enabled, the resulting index must stay a
    // working "lucene" dummy index (with includedPaths "/dummy"), not be
    // overridden back to type "disabled"
    @Test
    public void fullyCustomIndexRemovedCreatesDummyLuceneIndex() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetStateIndex.rightsManagement-1-custom-1": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "async": ["async", "nrt"],
                        "includedPaths": "/content/dam",
                        "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                        "mergeChecksum": "abc",
                        "merges": ["/oak:index/damAssetStateIndex.rightsManagement"],
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/diff.index": {
                        "jcr:primaryType": "nt:unstructured",
                        "type": "lucene", "includedPaths": "/a", "queryPaths": "/a"
                    }
                }
                """, true);
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        DiffIndexMerger merger = new DiffIndexMerger().
                setDeleteCopiesOutOfTheBoxIndex(false).
                setDeleteCreatesDummyIndex(true).
                setLogAtInfoLevel(true);
        merger.merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        JsonObject merged = newImageLuceneDefinitions;
        assertEquals("""
                {
                  "/oak:index/damAssetStateIndex.rightsManagement-1-custom-2": {
                    "async": "async",
                    "includedPaths": "/dummy",
                    "queryPaths": "/dummy",
                    "type": "lucene",
                    "jcr:primaryType": "nam:oak:QueryIndexDefinition",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "ea20eb28050ff4cd8d270298656ff7b80c7322aa7964c5c30c32756ef4de228a",
                    "merges": ["/oak:index/damAssetStateIndex.rightsManagement"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "properties": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "dummy": {
                          "name": "dummy",
                          "propertyIndex": true,
                          "jcr:primaryType": "nam:nt:unstructured"
                        }
                      }
                    }
                  }
                }""", merged.toString());
    }

    // GRANITE-69761: when a customization is removed from diff.index, and
    // setDeleteCopiesOutOfTheBoxIndex is enabled, then the original
    // (out-of-the-box) index must be copied
    @Test
    public void customizationRemovedCreatesCopyOfOriginalIndex() {
        JsonObject repositoryDefinitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetStateIndex-1": {
                        "async": [ "async", "nrt" ],
                        "includedPaths": "/content/dam",
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "lucene",
                        "indexRules": {
                            "dam:Asset": {
                                "properties": {
                                    "x": {
                                        "name": "x",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/damAssetStateIndex-1-custom-1": {
                        "async": [ "async", "nrt" ],
                        "includedPaths": "/content/dam",
                        "jcr:primaryType": "oak:IndexDefinition",
                        "mergeChecksum": "cf1177ccdf1b404eeb7787ba63c15fefa3fcaacdad38605f91cf00fa1aed0409",
                        "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                        "merges": [ "/oak:index/damAssetStateIndex" ],
                        "type": "lucene",
                        "indexRules": {
                            "jcr:primaryType": "nam:nt:unstructured",
                            "dam:Asset": {
                                "jcr:primaryType": "nam:nt:unstructured",
                                "properties": {
                                    "jcr:primaryType": "nam:nt:unstructured",
                                    "x": {
                                        "jcr:primaryType": "nam:nt:unstructured",
                                        "name": "x",
                                        "propertyIndex": true
                                    },
                                    "y": {
                                        "jcr:primaryType": "nam:nt:unstructured",
                                        "name": "y",
                                        "propertyIndex": true
                                    }
                                }
                            }
                        }
                    },
                    "/oak:index/diff.index": {
                        "includedPaths": "/same",
                        "jcr:primaryType": "nt:unstructured",
                        "queryPaths": "/same",
                        "type": "lucene",
                        "diff": {}
                    }
                }
                """, true);
        JsonObject newImageLuceneDefinitions = JsonObject.fromJson("{}", true);
        DiffIndexMerger merger = new DiffIndexMerger().
                setDeleteCopiesOutOfTheBoxIndex(false).
                setDeleteCreatesDummyIndex(true).
                setLogAtInfoLevel(true);
        merger.merge(newImageLuceneDefinitions, repositoryDefinitions, null);
        JsonObject merged = newImageLuceneDefinitions;
        assertEquals("""
                {
                  "/oak:index/damAssetStateIndex-1-custom-2": {
                    "async": ["async", "nrt"],
                    "includedPaths": "/content/dam",
                    "jcr:primaryType": "oak:IndexDefinition",
                    "type": "lucene",
                    "mergeInfo": "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html",
                    "mergeChecksum": "43402360ba9021820657ef60e2c5fcece9ac9a06e03848817cba6779c189f947",
                    "merges": ["/oak:index/damAssetStateIndex"],
                    "indexRules": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "dam:Asset": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "properties": {
                          "jcr:primaryType": "nam:nt:unstructured",
                          "x": {
                            "name": "x",
                            "propertyIndex": true,
                            "jcr:primaryType": "nam:nt:unstructured"
                          }
                        }
                      }
                    }
                  }
                }""", merged.toString());
    }

    // GRANITE-69361: an empty child node in the diff (eg. an analyzer filter
    // without any properties, such as "LowerCase": {}) must be kept as an
    // empty node - just getting a "jcr:primaryType" added - and not be turned
    // into a disabled/dummy index structure. That special handling (the
    // "target is empty" fallback in mergeInto()) is only meant for the
    // top-level index definition itself, not for arbitrary nested empty nodes.
    @Test
    public void emptyChildNodeIsPreservedNotDisabled() {
        JsonObject diff = JsonObject.fromJson("""
                {
                    "async": "async",
                    "compatVersion": 2,
                    "includedPaths": ["/content"],
                    "queryPaths": "/content",
                    "type": "lucene",
                    "analyzers": {
                        "default": {
                            "filters": {
                                "LowerCase": {}
                            }
                        }
                    },
                    "indexRules": {
                        "nt:unstructured": {
                            "properties": {
                                "code": {
                                    "name": "code",
                                    "propertyIndex": true
                                }
                            }
                        }
                    }
                }
                """, true);

        JsonObject merged = new DiffIndexMerger().processMerge("custom.test", null, diff);
        assertEquals("""
                {
                  "async": "async",
                  "compatVersion": 2,
                  "includedPaths": ["/content"],
                  "queryPaths": "/content",
                  "type": "lucene",
                  "jcr:primaryType": "nam:oak:QueryIndexDefinition",
                  "analyzers": {
                    "jcr:primaryType": "nam:nt:unstructured",
                    "default": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "filters": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "LowerCase": {
                          "jcr:primaryType": "nam:nt:unstructured"
                        }
                      }
                    }
                  },
                  "indexRules": {
                    "jcr:primaryType": "nam:nt:unstructured",
                    "nt:unstructured": {
                      "jcr:primaryType": "nam:nt:unstructured",
                      "properties": {
                        "jcr:primaryType": "nam:nt:unstructured",
                        "code": {
                          "name": "code",
                          "propertyIndex": true,
                          "jcr:primaryType": "nam:nt:unstructured"
                        }
                      }
                    }
                  }
                }""", merged.toString());
    }

    @Test
    public void removeDiffIndexEntriesNoOpWhenAbsent() {
        JsonObject definitions = JsonObject.fromJson("""
                {
                    "/oak:index/damAssetLucene-8": {
                        "jcr:primaryType": "oak:IndexDefinition",
                        "type": "elasticsearch"
                    }
                }
                """, true);

        DiffIndexMerger.removeDiffIndexEntries(definitions);
        assertEquals("[/oak:index/damAssetLucene-8]", definitions.getChildren().keySet().toString());
    }
}
