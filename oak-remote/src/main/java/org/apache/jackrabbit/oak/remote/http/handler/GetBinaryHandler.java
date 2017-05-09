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

package org.apache.jackrabbit.oak.remote.http.handler;

import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.remote.RemoteBinaryFilters;
import org.apache.jackrabbit.oak.remote.RemoteBinaryId;
import org.apache.jackrabbit.oak.remote.RemoteSession;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendBadRequest;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendInternalServerError;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendNotFound;

class GetBinaryHandler implements Handler {

    private static final String CONTENT_RANGE_HEADER = "Content-Range";

    private static final String RANGE_HEADER = "Range";

    private static final Pattern RANGE_HEADER_PATTERN = Pattern.compile("^\\s*bytes\\s*=\\s*(.*)\\s*$");

    private static final Pattern RANGE_PATTERN = Pattern.compile("^\\s*(\\d*)\\s*(?:\\s*-\\s*(\\d*))?\\s*$");

    private static final String MULTIPART_DELIMITER = "MULTIPART-DELIMITER";

    private static final Pattern REQUEST_PATTERN = Pattern.compile("^/binaries/(.*)$");

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        RemoteSession session = (RemoteSession) request.getAttribute("session");

        if (session == null) {
            sendInternalServerError(response, "session not found");
            return;
        }

        String providedBinaryId = readBinaryId(request);

        if (providedBinaryId == null) {
            sendBadRequest(response, "unable to read the provided binary ID");
            return;
        }

        RemoteBinaryId binaryId = session.readBinaryId(providedBinaryId);

        if (binaryId == null) {
            sendNotFound(response, "binary ID not found");
            return;
        }

        List<RemoteBinaryFilters> contentRanges = parseRequestRanges(request, session, binaryId);

        if (contentRanges == null) {
            handleFile(response, session, binaryId);
        } else if (contentRanges.size() == 1) {
            handleSingleRange(response, session, binaryId, contentRanges.get(0));
        } else {
            handleMultipleRanges(response, session, binaryId, contentRanges);
        }
    }

    /**
     * RFC7233
     * <p/>
     * This handler sends a 200 OK http status, the Content-Length header and
     * the entire file/binary content. This is used when the request Range
     * header is missing or it contains a malformed value.
     */
    private void handleFile(HttpServletResponse response, RemoteSession session, RemoteBinaryId binaryId) throws IOException {

        InputStream in = session.readBinary(binaryId, new RemoteBinaryFilters());

        long length = session.readBinaryLength(binaryId);

        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/octet-stream");
        response.setContentLength((int) length);

        OutputStream out = response.getOutputStream();

        ByteStreams.copy(in, out);

        out.close();
    }

    /**
     * RFC7233
     * <p/>
     * This handler sends a 206 Partial Content http status, the Content-Length
     * header, the Content-Range header and the requested binary fragment. This
     * is used when the request Range header contains only one range.
     */
    private void handleSingleRange(HttpServletResponse response, RemoteSession session, RemoteBinaryId binaryId, RemoteBinaryFilters range) throws IOException {
        InputStream in = session.readBinary(binaryId, range);

        long fileLength = session.readBinaryLength(binaryId);
        long rangeStart = range.getStart();
        long rangeEnd = rangeStart + range.getCount() - 1;

        response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
        response.setHeader(CONTENT_RANGE_HEADER, String.format("%d-%d/%d", rangeStart, rangeEnd, fileLength));
        response.setContentType("application/octet-stream");
        response.setContentLength((int) (rangeEnd - rangeStart + 1));

        OutputStream out = response.getOutputStream();

        ByteStreams.copy(in, out);

        out.close();
    }

    /**
     * RFC7233
     * <p/>
     * This handler sends a 206 Partial Content http status, the Content-Length
     * header, Content-Type multipart/byteranges The payload contains all the
     * requested binary fragments.
     * <p/>
     * This handler is used when multiple ranges are requested.
     */
    private void handleMultipleRanges(HttpServletResponse response, RemoteSession session, RemoteBinaryId binaryId, List<RemoteBinaryFilters> ranges) throws IOException {

        String header;

        long rangeStart, rangeEnd, fileLength, contentLength;

        fileLength = session.readBinaryLength(binaryId);

        // Compute response content length
        // Create multipart headers

        contentLength = 0;

        List<String> multipartHeaders = new ArrayList<String>(ranges.size());

        for (RemoteBinaryFilters range : ranges) {
            rangeStart = range.getStart();
            rangeEnd = rangeStart + range.getCount() - 1;

            header = String.format("\n" +
                            "--%s\n" +
                            "Content-Type: application/octet-stream" +
                            "Content-Content-Range: %d-%d/%d\n\n",
                    MULTIPART_DELIMITER, rangeStart, rangeEnd, fileLength);

            multipartHeaders.add(header);

            contentLength += header.getBytes().length;
            contentLength += range.getCount();
        }

        // Send response status and headers

        response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
        response.setContentLength((int) contentLength);
        response.setContentType("multipart/byteranges; boundary=" + MULTIPART_DELIMITER);

        // Send requested ranges

        RemoteBinaryFilters range;

        InputStream in;

        OutputStream out = response.getOutputStream();

        Iterator<RemoteBinaryFilters> rangeIt = ranges.iterator();
        Iterator<String> headerIt = multipartHeaders.iterator();

        while (rangeIt.hasNext() && headerIt.hasNext()) {
            range = rangeIt.next();
            header = headerIt.next();

            out.write(header.getBytes());
            in = session.readBinary(binaryId, range);
            ByteStreams.copy(in, out);
        }

        out.close();
    }

    /**
     * Extract binary id from request path and return it
     */
    private String readBinaryId(HttpServletRequest request) {
        Matcher matcher = REQUEST_PATTERN.matcher(request.getPathInfo());

        if (matcher.matches()) {
            return matcher.group(1);
        }

        throw new IllegalStateException("handler bound at the wrong path");
    }

    /**
     * This method parses the request Range header a list of ranges as
     * RemoteBinaryFilters ( or null when the header is missing or contains
     * invalid/malformed values
     */
    private List<RemoteBinaryFilters> parseRequestRanges(HttpServletRequest request, RemoteSession session, RemoteBinaryId binaryId) {

        // Check header exists
        String headerValue = request.getHeader(RANGE_HEADER);

        if (headerValue == null) {
            return null;
        }

        // Check header is bytes=*
        Matcher matcher = RANGE_HEADER_PATTERN.matcher(headerValue);

        if (!matcher.matches()) {
            return null;
        }

        // Iterate requested ranges
        headerValue = matcher.group(1);

        StringTokenizer tokenizer = new StringTokenizer(headerValue, ",");

        List<RemoteBinaryFilters> ranges = new LinkedList<RemoteBinaryFilters>();

        RemoteBinaryFilters range;

        long fileLength = session.readBinaryLength(binaryId);

        while (tokenizer.hasMoreTokens()) {
            range = parseRange(tokenizer.nextToken(), fileLength);

            if (range == null) {
                return null;
            }

            ranges.add(range);
        }

        return ranges;
    }

    /**
     * Parse a range extracted from the Range header and return a wrapped
     * RemoteBinaryFilters instance for the range or null if the range is not
     * valid or malformed.
     * <p/>
     * The returned RemoteBinaryFilters object will never return -1 in
     * getCount.
     */
    private RemoteBinaryFilters parseRange(String range, long fileLength) {
        Matcher matcher = RANGE_PATTERN.matcher(range);

        if (!matcher.matches()) {
            return null;
        }

        final long start;
        final long end;

        // Content-Range: X
        if (matcher.group(2) == null || matcher.group(2).isEmpty()) {
            start = Long.parseLong(matcher.group(1));
            end = fileLength - 1;
        }
        // Content-Range: -X
        else if (matcher.group(1).isEmpty()) {
            end = fileLength - 1;
            start = end - Long.parseLong(matcher.group(2)) + 1;
        }
        // Content-Range: X-Y
        else {
            start = Long.parseLong(matcher.group(1));
            end = Long.parseLong(matcher.group(2));
        }

        // Simple range validation
        if (start < 0 || end < 0 || start > end || end >= fileLength || start >= fileLength) {
            return null;
        }

        return new RemoteBinaryFilters() {
            @Override
            public long getStart() {
                return start;
            }

            @Override
            public long getCount() {
                return end - start + 1;
            }
        };
    }
}
