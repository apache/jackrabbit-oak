package org.apache.jackrabbit.oak.plugins.index.search.spi.editor;

import static org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextDocumentMaker.LOG_TRUNCATION_LENGTH;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MultiLongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.junit.Test;

public class LuceneTruncateLargeValuesTest {

    @Test
    public void testSingleValuePropertyTruncation() {
        // empty string
        PropertyState emptyStringProperty = StringPropertyState.stringProperty("empty", "");
        assertEquals("", FulltextDocumentMaker.truncateForLogging(emptyStringProperty));

        // no truncation here as the value doesn't exceed the truncation size
        PropertyState shortStringProperty = StringPropertyState.stringProperty("short", "value");
        assertEquals("value", FulltextDocumentMaker.truncateForLogging(shortStringProperty));

        // no truncation here as the value is exactly the truncation size
        PropertyState exactSizeString = StringPropertyState.stringProperty("exact",
            "a".repeat(LOG_TRUNCATION_LENGTH));
        assertEquals("a".repeat(LOG_TRUNCATION_LENGTH),
            FulltextDocumentMaker.truncateForLogging(exactSizeString));

        // Truncation here as the value exceeds the truncation size
        PropertyState longerString = StringPropertyState.stringProperty("exceed",
            "a".repeat(LOG_TRUNCATION_LENGTH + 10));
        assertEquals("a".repeat(LOG_TRUNCATION_LENGTH) + "...",
            FulltextDocumentMaker.truncateForLogging(longerString));
    }

    @Test
    public void testTruncateForLogging() {
        // empty list property
        PropertyState emptyListProperty = MultiStringPropertyState.emptyProperty("empty",
            Type.STRINGS);
        assertEquals("[]", FulltextDocumentMaker.truncateForLogging(emptyListProperty));

        // 200 elements of "x". This list's string representation is longer than the truncation length.
        // So it will be a string "[x, x, x, x...]" whose length is the truncation length + 1 since we
        // append a "]" at the end.
        List<String> manyRepeated = Collections.nCopies(200, "x");
        PropertyState simpleMultiStringProperty = MultiStringPropertyState.stringProperty(
            "repeated", manyRepeated);
        String expected = manyRepeated.toString().substring(0, LOG_TRUNCATION_LENGTH) + "..." + "]";
        assertEquals(expected, FulltextDocumentMaker.truncateForLogging(simpleMultiStringProperty));

        List<Long> numbers = LongStream.rangeClosed(0, 1000).boxed().collect(Collectors.toList());
        PropertyState listLongProperty = MultiLongPropertyState.createLongProperty("numbers",
            numbers);
        expected = numbers.toString().substring(0, LOG_TRUNCATION_LENGTH) + "..." + "]";
        assertEquals(expected, FulltextDocumentMaker.truncateForLogging(listLongProperty));

        // few
        List<String> longStrings = List.of("x".repeat(100), "y".repeat(20), "z".repeat(100));
        PropertyState longStringsInList = MultiStringPropertyState.stringProperty("longStrings",
            longStrings);
        expected = longStrings.toString().substring(0, LOG_TRUNCATION_LENGTH) + "..." + "]";
        assertEquals(expected, FulltextDocumentMaker.truncateForLogging(longStringsInList));
    }
}
