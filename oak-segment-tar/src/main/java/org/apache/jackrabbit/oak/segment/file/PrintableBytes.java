package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

/**
 * An amount of bytes that is also pretty-printable for usage in logs.
 */
class PrintableBytes {

    /**
     * Create a new instance a {@link PrintableBytes}.
     *
     * @param bytes The amount of bytes.
     * @return A new instance of {@link PrintableBytes}.
     */
    static PrintableBytes newPrintableBytes(long bytes) {
        return new PrintableBytes(bytes);
    }

    private final long bytes;

    private PrintableBytes(long bytes) {
        this.bytes = bytes;
    }

    @Override
    public String toString() {
        return String.format("%s (%d bytes)", humanReadableByteCount(bytes), bytes);
    }

}

