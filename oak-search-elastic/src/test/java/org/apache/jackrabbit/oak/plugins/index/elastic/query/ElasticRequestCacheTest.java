package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ElasticRequestCacheTest {

    @Test
    public void withinWindow() {
        // Given
        Duration window = Duration.ofSeconds(20);

        Instant start = Instant.parse("2026-01-27T14:00:00Z");
        Instant second = start.plusMillis(100);
        Instant third = second.plusMillis(300);
        Instant fourth = third.plusMillis(100);

        // When
        ElasticRequestCache.MovingAverage average = new ElasticRequestCache.MovingAverage(start);
        average.step(window, second);
        average.step(window, third);
        average.step(window, fourth);

        // Then
        assertEquals(start, average.lastStart);
        assertEquals(fourth, average.lastEnd);
        assertEquals(3, average.approxCount);
        assertEquals(1000 * 3 / 500, average.currentRate());
    }

    @Test
    public void reset() {
        // Given
        Duration window = Duration.ofSeconds(20);

        Instant start = Instant.parse("2026-01-27T14:00:00Z");
        Instant second = start.plusMillis(100);
        Instant third = second.plusMillis(300);
        Instant fourth = third.plusMillis(100);
        Instant reset = fourth.plus(window.plusMillis(100));

        // When
        ElasticRequestCache.MovingAverage average = new ElasticRequestCache.MovingAverage(start);
        average.step(window, second);
        average.step(window, third);
        average.step(window, fourth);
        average.step(window, reset);

        // Then
        assertNull(average.lastStart);
        assertEquals(reset, average.lastEnd);
        assertEquals(0, average.approxCount);
        assertEquals(0, average.currentRate());
    }

    @Test
    public void approximationClose() {
        // Given
        Duration window = Duration.ofSeconds(1);

        Instant start = Instant.parse("2026-01-27T14:00:00Z");
        List<Integer> initialSteps = List.of(100, 200, 500, 700, 900);
        ElasticRequestCache.MovingAverage average = prepareAverage(window, start, initialSteps);

        int followingStep = 1100;

        // When
        average.step(window,start.plusMillis(followingStep));

        // Then
        assertEquals(5, average.approxCount);
    }

    @Test
    public void approximationFar() {
        // Given
        Duration window = Duration.ofSeconds(1);

        Instant start = Instant.parse("2026-01-27T14:00:00Z");
        List<Integer> initialSteps = List.of(100, 200, 500, 700, 900);
        ElasticRequestCache.MovingAverage average = prepareAverage(window, start, initialSteps);

        int followingStep = 1700;

        // When
        average.step(window,start.plusMillis(followingStep));

        // Then
        assertEquals(2, average.approxCount);
    }

    private ElasticRequestCache.MovingAverage prepareAverage(Duration window, Instant start, List<Integer> steps) {
        var average = new ElasticRequestCache.MovingAverage(start);
        for (Integer step : steps) {
            average.step(window, start.plusMillis(step));
        }
        return average;
    }
}
