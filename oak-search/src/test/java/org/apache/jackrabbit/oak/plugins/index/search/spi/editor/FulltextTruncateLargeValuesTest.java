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

public class FulltextTruncateLargeValuesTest {

    @Test
    public void testSingleValueTruncation() {
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
        PropertyState exceedingTruncationSizeString = StringPropertyState.stringProperty(
            "exceedingTruncationSize",
            "a".repeat(LOG_TRUNCATION_LENGTH + 10));
        assertEquals("a".repeat(LOG_TRUNCATION_LENGTH) + "...",
            FulltextDocumentMaker.truncateForLogging(exceedingTruncationSizeString));
    }

    @Test
    public void testMultiValueTruncation() {
        // empty list property
        PropertyState emptyListProperty = MultiStringPropertyState.emptyProperty("empty",
            Type.STRINGS);
        assertEquals("[]", FulltextDocumentMaker.truncateForLogging(emptyListProperty));

        // 200 elements of "x". This list's string representation "[x, x, x, x...]" has a length
        // longer than the truncation limit.
        List<String> manyRepeated = Collections.nCopies(200, "x");
        PropertyState simpleMultiStringProperty = MultiStringPropertyState.stringProperty(
            "repeated", manyRepeated);
        // expect the string to be a string "[x, x, x, x...]" that has a length of 128.
        String expected =
            Collections.nCopies(120, "x").toString().substring(0, LOG_TRUNCATION_LENGTH) + "..."
                + "]";
        assertEquals(expected, FulltextDocumentMaker.truncateForLogging(simpleMultiStringProperty));

        List<Long> numbers = LongStream.rangeClosed(0, 1000).boxed().collect(Collectors.toList());
        PropertyState listLongProperty = MultiLongPropertyState.createLongProperty("numbers",
            numbers);
        expected = numbers.toString().substring(0, LOG_TRUNCATION_LENGTH) + "..." + "]";
        assertEquals(expected, FulltextDocumentMaker.truncateForLogging(listLongProperty));

        List<String> longStrings = List.of("x".repeat(100), "y".repeat(20), "z".repeat(100));
        PropertyState longStringsInList = MultiStringPropertyState.stringProperty("longStrings",
            longStrings);
        expected = longStrings.toString().substring(0, LOG_TRUNCATION_LENGTH) + "..." + "]";
        assertEquals(expected, FulltextDocumentMaker.truncateForLogging(longStringsInList));
    }
}
