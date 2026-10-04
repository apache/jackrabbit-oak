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
package org.apache.jackrabbit.oak.segment.http.server.handlers;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.http.server.util.ApiErrorUtil;
import org.apache.jackrabbit.oak.segment.http.server.util.FormatUtils;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Handler for explorer API endpoints (/api/explore, /api/segments/recent, /api/segments/tars).
 * 
 * <p>Extracted from SegmentHttpServer to separate API exploration concerns.</p>
 */
public class ExplorerApiHandler {
    private static final Logger log = LoggerFactory.getLogger(ExplorerApiHandler.class);
    
    private final NodeStore nodeStore;
    private final Path storeDirectory;
    private final Supplier<org.apache.jackrabbit.oak.spi.blob.BlobStore> blobStoreSupplier;
    
    public ExplorerApiHandler(NodeStore nodeStore, Path storeDirectory, org.apache.jackrabbit.oak.spi.blob.BlobStore blobStore) {
        this(nodeStore, storeDirectory, () -> blobStore, true);
    }

    private ExplorerApiHandler(
            NodeStore nodeStore,
            Path storeDirectory,
            Supplier<org.apache.jackrabbit.oak.spi.blob.BlobStore> blobStoreSupplier,
            boolean ignored) {
        this.nodeStore = nodeStore;
        this.storeDirectory = storeDirectory;
        this.blobStoreSupplier = blobStoreSupplier;
    }

    public static ExplorerApiHandler withBlobStoreSupplier(
            NodeStore nodeStore,
            Path storeDirectory,
            Supplier<org.apache.jackrabbit.oak.spi.blob.BlobStore> blobStoreSupplier) {
        return new ExplorerApiHandler(nodeStore, storeDirectory, blobStoreSupplier, true);
    }
    
    /**
     * Handle GET /api/explore?path={path} - Browse node tree structure (JSON).
     */
    public void handleExploreNode(HttpServletResponse response, String path) throws IOException {
        response.setContentType("application/json");
        
        try {
            // Get the head state from NodeStore
            NodeState root = nodeStore.getRoot();
            
            // Navigate to the requested path
            NodeState node = root;
            if (!"/".equals(path)) {
                String[] parts = path.substring(1).split("/");
                for (String part : parts) {
                    if (!part.isEmpty()) {
                        node = node.getChildNode(part);
                        if (!node.exists()) {
                            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, "Node not found");
                            return;
                        }
                    }
                }
            }
            
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("path", path);
            List<String> children = new ArrayList<>();
            for (String childName : node.getChildNodeNames()) {
                if (!NodeStateUtils.isHidden(childName)) {
                    children.add(childName);
                }
            }
            payload.put("children", children);
            Map<String, Object> props = new LinkedHashMap<>();
            
            // Iterate through actual properties
            for (PropertyState prop : node.getProperties()) {
                String propName = prop.getName();
                
                // Handle different property types
                if (prop.isArray()) {
                    List<String> values = new ArrayList<>();
                    for (int i = 0; i < prop.count(); i++) {
                        values.add(String.valueOf(prop.getValue(Type.STRING, i)));
                    }
                    props.put(propName, values);
                } else {
                    // Single value - handle different types
                    try {
                        String value;
                        if (prop.getType() == Type.BINARY) {
                            long binarySize = prop.size();
                            // Try to get blob ID which might be an IPFS CID
                            try {
                                org.apache.jackrabbit.oak.api.Blob blob = prop.getValue(Type.BINARY);
                                String blobId = blob.getContentIdentity();
                                if (blobId != null && (blobId.startsWith("Qm") || blobId.startsWith("bafy"))) {
                                    // IPFS CID detected!
                                    value = "ipfs://" + blobId;
                                } else if (blobId != null) {
                                    value = "[Binary: " + binarySize + " bytes, id=" + blobId + "]";
                                } else {
                                    value = "[Binary: " + binarySize + " bytes]";
                                }
                            } catch (Exception blobEx) {
                                value = "[Binary: " + binarySize + " bytes]";
                            }
                        } else if (prop.getType() == Type.BOOLEAN) {
                            value = String.valueOf(prop.getValue(Type.BOOLEAN));
                        } else if (prop.getType() == Type.LONG) {
                            value = String.valueOf(prop.getValue(Type.LONG));
                        } else if (prop.getType() == Type.DOUBLE) {
                            value = String.valueOf(prop.getValue(Type.DOUBLE));
                        } else if (prop.getType() == Type.DATE) {
                            value = String.valueOf(prop.getValue(Type.DATE));
                        } else {
                            value = prop.getValue(Type.STRING);
                        }
                        props.put(propName, value);
                    } catch (Exception e) {
                        props.put(propName, "[Error: " + e.getMessage() + "]");
                    }
                }
            }
            payload.put("properties", props);
            
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(payload));
            
        } catch (Exception e) {
            log.error("Error exploring node: " + path, e);
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                FormatUtils.escapeJson(e.getMessage()));
        }
    }
    
    /**
     * Handle GET /api/segments/recent - Recent segment writes from journal (JSON).
     */
    public void handleRecentSegments(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        try {
            List<Map<String, Object>> segments = new ArrayList<>();
            Path journalPath = storeDirectory.resolve("journal.log");
            
            if (Files.exists(journalPath)) {
                List<String> lines = Files.readAllLines(journalPath);
                int start = Math.max(0, lines.size() - 20);
                for (int i = lines.size() - 1; i >= start; i--) {
                    String line = lines.get(i);
                    if (line.contains(" ")) {
                        String[] parts = line.split(" ", 2);
                        Map<String, Object> segment = new LinkedHashMap<>();
                        segment.put("id", parts[0]);
                        segment.put("timestamp", parts.length > 1 ? parts[1] : "");
                        segments.add(segment);
                    }
                }
            }
            
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(segments));
            
        } catch (Exception e) {
            log.error("Error reading recent segments", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("[]");
        }
    }
    
    /**
     * Handle GET /api/segments/tars - List all TAR files and storage blocks (JSON).
     */
    public void handleTarFiles(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        
        try {
            List<Map<String, Object>> tarEntries = new ArrayList<>();
            
            // Count total segments in journal
            int totalSegments = 0;
            Path journalPath = storeDirectory.resolve("journal.log");
            if (Files.exists(journalPath)) {
                List<String> journalLines = Files.readAllLines(journalPath);
                totalSegments = journalLines.size();
            }
            
            // List all .tar files and calculate total size
            List<Path> tarFiles = new java.util.ArrayList<>();
            long totalSize = 0;
            try (java.util.stream.Stream<Path> paths = Files.list(storeDirectory)) {
                tarFiles = paths
                    .filter(p -> p.toString().endsWith(".tar"))
                    .sorted(java.util.Comparator.comparing(Path::toString))
                    .collect(Collectors.toList());
                for (Path tarFile : tarFiles) {
                    totalSize += Files.size(tarFile);
                }
            }
            
            // Build JSON entries
            for (Path tarFile : tarFiles) {
                String fileName = tarFile.getFileName().toString();
                long fileSize = Files.size(tarFile);
                java.nio.file.attribute.BasicFileAttributes attrs = 
                    Files.readAttributes(tarFile, java.nio.file.attribute.BasicFileAttributes.class);
                
                // Estimate segment count based on proportional file size
                int estimatedSegments = totalSize > 0 ? (int)((fileSize * totalSegments) / totalSize) : 0;
                
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("name", fileName);
                entry.put("size", fileSize);
                entry.put("sizeFormatted", FormatUtils.formatBytes(fileSize));
                entry.put("segmentCount", estimatedSegments);
                entry.put("estimatedCount", true);
                entry.put("created", attrs.creationTime().toString());
                entry.put("modified", attrs.lastModifiedTime().toString());
                tarEntries.add(entry);
            }
            
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(JsonOutputUtil.toJson(tarEntries));
            
        } catch (Exception e) {
            log.error("Error reading TAR files", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("[]");
        }
    }
    
    /**
     * Handle GET /api/blob/{blobId} - Stream binary from Oak BlobStore.
     * 
     * <p>This endpoint allows streaming binaries that are stored in Oak BlobStore
     * but don't have IPFS CID mappings yet. Useful for genesis content and testing.</p>
     * 
     * @param request HTTP request
     * @param response HTTP response
     * @param blobId Oak blob ID (e.g., ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442)
     */
    public void handleBlobStream(jakarta.servlet.http.HttpServletRequest request, 
                                  HttpServletResponse response, 
                                  String blobId) throws IOException {
        org.apache.jackrabbit.oak.spi.blob.BlobStore blobStore = blobStoreSupplier.get();
        if (blobStore == null) {
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, 
                "BlobStore not configured");
            return;
        }
        
        if (blobId == null || blobId.isEmpty()) {
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_BAD_REQUEST, "Blob ID required");
            return;
        }
        
        try {
            log.info("📦 Streaming blob from BlobStore: {}", blobId);
            
            // Read blob from BlobStore
            java.io.InputStream blobStream = blobStore.getInputStream(blobId);
            
            if (blobStream == null) {
                log.warn("Blob not found in BlobStore: {}", blobId);
                ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_NOT_FOUND, 
                    "Blob not found: " + blobId);
                return;
            }
            
            // Try to determine content type from blob ID or default to octet-stream
            // For genesis image, we know it's JPEG
            String contentType = "application/octet-stream";
            if (blobId.contains("do-it-live") || blobId.startsWith("ed06f9cb")) {
                contentType = "image/jpeg";
            }
            
            // Set response headers
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(contentType);
            response.setHeader("Cache-Control", "public, max-age=31536000"); // 1 year cache
            response.setHeader("X-Blob-Id", blobId);
            
            // Stream the blob
            try (java.io.OutputStream out = response.getOutputStream()) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                long totalBytes = 0;
                
                while ((bytesRead = blobStream.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                    totalBytes += bytesRead;
                }
                
                out.flush();
                log.info("✅ Streamed blob {} ({} bytes)", blobId, totalBytes);
            } finally {
                blobStream.close();
            }
            
        } catch (Exception e) {
            log.error("Error streaming blob {}: {}", blobId, e.getMessage());
            ApiErrorUtil.sendJsonError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 
                "Failed to stream blob: " + e.getMessage());
        }
    }
}
