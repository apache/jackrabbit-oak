package org.apache.jackrabbit.oak.plugins.memory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.BOOLEANS;
import static org.apache.jackrabbit.oak.api.Type.DECIMAL;
import static org.apache.jackrabbit.oak.api.Type.DECIMALS;
import static org.apache.jackrabbit.oak.api.Type.DOUBLE;
import static org.apache.jackrabbit.oak.api.Type.DOUBLES;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * The methods of this class adapt a {@code PropertyState} to a {@code CoreValue}.
 * TODO this is a temporary solution while resolving OAK-350
 */
public class CoreValues {
    private CoreValues() {}

    /**
     * Value of the {@code property}
     * @param property
     * @return The single value of {@code property}.
     * @throws IllegalStateException if {@code property.isArray()} is {@code true}.
     */
    @Nonnull
    public static CoreValue getValue(PropertyState property) {
        if (property.isArray()) {
            throw new IllegalStateException("Not a single valued property");
        }

        int type = property.getType().tag();
        switch (type) {
            case PropertyType.STRING: return new StringValue(property.getValue(STRING));
            case PropertyType.LONG: return new LongValue(property.getValue(LONG));
            case PropertyType.DOUBLE: return new DoubleValue(property.getValue(DOUBLE));
            case PropertyType.BOOLEAN: return property.getValue(BOOLEAN) ? BooleanValue.TRUE : BooleanValue.FALSE;
            case PropertyType.DECIMAL: return new DecimalValue(property.getValue(DECIMAL));
            case PropertyType.BINARY: return binaryValue(property.getValue(BINARY));
            default: return new GenericValue(type, property.getValue(STRING));
        }
    }

    /**
     * Values of  {@code property}. The returned list is immutable and contains
     * all the values of the property. If {@code property} is a single-valued property,
     * then the returned list will simply contain a single value.
     * @param property
     * @return immutable list of the values of this property
     */
    @Nonnull
    public static List<CoreValue> getValues(PropertyState property) {
        List<CoreValue> cvs = Lists.newArrayList();
        int type = property.getType().tag();
        switch (type) {
            case PropertyType.STRING:
                for (String value : property.getValue(STRINGS)) {
                    cvs.add(new StringValue(value));
                }
                break;
            case PropertyType.LONG:
                for (long value : property.getValue(LONGS)) {
                    cvs.add(new LongValue(value));
                }
                break;
            case PropertyType.DOUBLE:
                for (double value : property.getValue(DOUBLES)) {
                    cvs.add(new DoubleValue(value));
                }
                break;
            case PropertyType.BOOLEAN:
                for (boolean value : property.getValue(BOOLEANS)) {
                    cvs.add(value ? BooleanValue.TRUE : BooleanValue.FALSE);
                }
                break;
            case PropertyType.DECIMAL:
                for (BigDecimal value : property.getValue(DECIMALS)) {
                    cvs.add(new DecimalValue(value));
                }
                break;
            case PropertyType.BINARY:
                for (Blob value : property.getValue(BINARIES)) {
                    cvs.add(binaryValue(value));
                }
                break;
            default:
                for (String value : property.getValue(STRINGS)) {
                    cvs.add(new GenericValue(type, value));
                }
        }
        return cvs;
    }

    private static CoreValue binaryValue(Blob blob) {
        InputStream in = blob.getNewStream();
        try {
            return new BinaryValue(ByteStreams.toByteArray(in));
        }
        catch (IOException e) {
            // TODO better return a stream which defers this exception until accessed
            throw new IllegalStateException(e);
        }
    }

}
