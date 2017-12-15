package org.apache.jackrabbit.oak.segment.file;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

/**
 * A stopwatch that is also pretty-printable for usage in log messages.
 */
class PrintableStopwatch {

    /**
     * Create a new {@link PrintableStopwatch} and starts it.
     *
     * @return A new instance of {@link PrintableStopwatch}.
     */
    static PrintableStopwatch createStarted() {
        return new PrintableStopwatch(Stopwatch.createStarted());
    }

    private final Stopwatch stopwatch;

    private PrintableStopwatch(Stopwatch stopwatch) {
        this.stopwatch = stopwatch;
    }

    @Override
    public String toString() {
        return String.format("%s (%d ms)", stopwatch, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

}

