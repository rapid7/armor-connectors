package com.rapid7.presto.armor;

import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Marker.Bound;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.rapid7.armor.read.predicate.InstantPredicate;
import com.rapid7.armor.read.predicate.NumericPredicate;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.store.Operator;

import io.airlift.slice.Slice;

public final class ArmorDomainUtil {
    private ArmorDomainUtil() {}

    private static String valueToString(Object value) {
        if (value != null) {
            if (value instanceof Slice)
              return ((Slice)value).toStringUtf8();
            else if (value instanceof String)
              return (String) value;
        }
        return null;
    }
    
    private static Number valueToNumber(Object value) {
        if (value != null) {
            if (value instanceof Number)
                return (Number) value;
        }
        return null;
    }
    
    private static Object getSingleValue(Range range) {
        if (range.getLow().getValueBlock().isPresent())
            return range.getLow().getValue();
        else
            return range.getHigh().getValue();
    }
    
    
    private static Operator determineOperator(List<Range> ranges, List<?> values) {
        if (ranges.size() < 2)
            throw new IllegalArgumentException("This should only be called if there is more than 1 range");
        // Not equals or Not in should have some form of unset/unbounded min max.
        boolean minUnbounded = false;
        boolean maxUnbounded = false;
        
        for (Range range : ranges) {
            if (range.getHigh().isLowerUnbounded())
                minUnbounded = true;
            if (range.getHigh().isUpperUnbounded())
                maxUnbounded = true;
            if (range.getLow().isLowerUnbounded())
                minUnbounded = true;
            if (range.getLow().isUpperUnbounded())
                maxUnbounded = true;
        }
        // NOTE: If we need to differentiate between != and NOT_IN then use values to find out.
        if (minUnbounded && maxUnbounded)
            return Operator.NOT_EQUALS;
        else
            return Operator.IN;
    }
    
    public static NumericPredicate<?> columnNumericPredicate(String field, Domain predicate) {
        ValueSet values = predicate.getValues();
        if (values instanceof SortedRangeSet) {
            SortedRangeSet srs = (SortedRangeSet) values;
            List<Range> ranges = srs.getOrderedRanges();
            if (ranges.isEmpty()) {
                return new NumericPredicate<Number>(field, predicate.isNullAllowed() ? Operator.IS_NULL : Operator.NOT_NULL, Collections.emptyList());
            } else if (ranges.size() > 1) {
                // In (2 values or more), NOT IN or != clause style predicate
                List<Number> numberValues = new ArrayList<>();
                for (Range range : ranges) {
                   Number value = valueToNumber(getSingleValue(range));
                   numberValues.add(value);
                }
                Operator operator = determineOperator(ranges, numberValues);
                return new NumericPredicate<Number>(field, operator, numberValues);
            } else {
                // Greater, less, between, IN (1 value) etc. NOTE: Equals not supported in presto on timestmp.
                Range range = ranges.iterator().next();                
                Operator operator = null;
                Marker highMarker = range.getHigh();
                Marker lowMarker = range.getLow();
                // For < and <=
                if (highMarker.getValueBlock().isPresent() && !lowMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE )
                        operator = Operator.LESS_THAN;
                    else if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.LESS_THAN_EQUAL;
                    Number interval = valueToNumber(highMarker.getValue());
                    return new NumericPredicate<Number>(field, Operator.IN, interval);
                }
                // For > and >=
                if (lowMarker.getValueBlock().isPresent() && !highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.EXACTLY )
                        operator = Operator.GREATER_THAN_EQUAL;
                    else if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.GREATER_THAN;
                    Number interval = valueToNumber(lowMarker.getValue());
                    return new NumericPredicate<Number>(field, operator, interval);
                }
                // For = or between
                if (highMarker.getValueBlock().isPresent() && lowMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.EXACTLY) {
                       // Between and equal are the same only way to determine the difference is if the values are different.
                        Number highValue = valueToNumber(highMarker.getValue());
                        Number lowValue = valueToNumber(lowMarker.getValue());
                       if (highValue.equals(lowValue)) {
                         return new NumericPredicate<Number>(field, Operator.EQUALS, highValue);
                       } else {
                         return new NumericPredicate<Number>(
                             field,
                             Operator.BETWEEN,
                             Arrays.asList(highValue, lowValue));
                       }
                    }
                } else {
                    return new NumericPredicate<Number>(field, predicate.isNullAllowed() ? Operator.IS_NULL : Operator.NOT_NULL, Collections.emptyList());
                }
            }
        }
        throw new RuntimeException("Unable to resolve predicate into armor predicate");
    }
    
    public static StringPredicate columnStringPredicate(String field, Domain predicate) {
        ValueSet values = predicate.getValues();
        if (values instanceof SortedRangeSet) {
            SortedRangeSet srs = (SortedRangeSet) values;
            List<Range> ranges = srs.getOrderedRanges();
            if (ranges.isEmpty()) {
                return new StringPredicate(field, predicate.isNullAllowed() ? Operator.IS_NULL : Operator.NOT_NULL, "");
            } else if (ranges.size() > 1) {
                // In clause style predicate
                List<String> stringValues = new ArrayList<>();
                for (Range range : ranges) {
                   String interval = valueToString(getSingleValue(range));
                   stringValues.add(interval);
                }
                Operator operator = determineOperator(ranges, stringValues);
                return new StringPredicate(field, operator, stringValues);
            } else {
                // Greater, less, between etc. NOTE: Equals not supported in presto on timestmp.q
                Range range = ranges.iterator().next();                
                Operator operator = null;
                Marker highMarker = range.getHigh();
                Marker lowMarker = range.getLow();
                // For < and <=
                if (highMarker.getValueBlock().isPresent() && !lowMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE )
                        operator = Operator.LESS_THAN;
                    else if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.LESS_THAN_EQUAL;
                    String interval = valueToString(highMarker.getValue());
                    return new StringPredicate(field, operator, interval);
                }
                // For > and >=
                if (lowMarker.getValueBlock().isPresent() && !highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.EXACTLY )
                        operator = Operator.GREATER_THAN_EQUAL;
                    else if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.GREATER_THAN;
                    String interval = valueToString(lowMarker.getValue());
                    return new StringPredicate(field, operator, interval);
                }
                // For = or between
                if (highMarker.getValueBlock().isPresent() && lowMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.EXACTLY) {
                       // Between and equal are the same only way to determine the dif qference is if the values are different.
                       String highValue = valueToString(highMarker.getValue());
                       String lowValue = valueToString(lowMarker.getValue());
                       if (highValue.equals(lowValue)) {
                         return new StringPredicate(field, Operator.EQUALS, highValue);
                       } else {
                         return new StringPredicate(
                             field,
                             Operator.BETWEEN,
                             Arrays.asList(highValue, lowValue));
                       }
                    }
                } else {
                    return new StringPredicate(field, predicate.isNullAllowed() ? Operator.IS_NULL : Operator.NOT_NULL, "");
                }
            }
        }
        throw new RuntimeException("Unable to resolve predicate into armor predicate");
    }
 
    public static StringPredicate intervalPredicate(Domain predicate) {
        ValueSet values = predicate.getValues();
        if (values instanceof SortedRangeSet) {
            SortedRangeSet srs = (SortedRangeSet) values;
            List<Range> ranges = srs.getOrderedRanges();
            if (ranges.size() > 1) {
                // In clause style predicate
                List<String> stringValues = new ArrayList<>();
                for (Range range : ranges) {
                   String interval = valueToString(getSingleValue(range));
                   stringValues.add(interval);
                }
                Operator operator = determineOperator(ranges, stringValues);
                return new StringPredicate(ArmorConstants.INTERVAL, operator, stringValues);
            } else {
                // Greater, less, between etc. NOTE: Equals not supported in presto on timestmp.
                Range range = ranges.iterator().next();                
                Operator operator = null;
                Marker highMarker = range.getHigh();
                Marker lowMarker = range.getLow();
                // For < and <=
                if (highMarker.getValueBlock().isPresent() && !lowMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE )
                        operator = Operator.LESS_THAN;
                    else if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.LESS_THAN_EQUAL;
                    String interval = valueToString(highMarker.getValue());
                    return new StringPredicate(ArmorConstants.INTERVAL, operator, interval);
                }
                // For > and >=
                if (lowMarker.getValueBlock().isPresent() && !highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.EXACTLY )
                        operator = Operator.GREATER_THAN_EQUAL;
                    else if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.GREATER_THAN;
                    String interval = valueToString(lowMarker.getValue());
                    return new StringPredicate(ArmorConstants.INTERVAL, operator, interval);
                }
                // For = or between
                if (highMarker.getValueBlock().isPresent() && highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.EXACTLY) {
                       // Between and equal are the same only way to determine the difference is if the values are different.
                       String highValue = valueToString(highMarker.getValue());
                       String lowValue = valueToString(lowMarker.getValue());
                       if (highValue.equals(lowValue)) {
                         return new StringPredicate(ArmorConstants.INTERVAL, Operator.EQUALS, highValue);
                       } else {
                         return new StringPredicate(
                             ArmorConstants.INTERVAL,
                             Operator.BETWEEN,
                             Arrays.asList(highValue, lowValue));
                       }
                    }
                }
            }
        }
        throw new RuntimeException("Unable to resolve predicate into armor predicate");
    }
    
    public static InstantPredicate startIntervalPredicate(Domain predicate) {
        ValueSet values = predicate.getValues();
        if (values instanceof SortedRangeSet) {
            SortedRangeSet srs = (SortedRangeSet) values;
            List<Range> ranges = srs.getOrderedRanges();
            if (ranges.size() > 1) {
                // In clause style predicate
                List<Instant> instantValues = new ArrayList<>();
                for (Range range : ranges) {
                   Long time = extractTimeValue(range.getLow()); // Range values are single set for low.
                   instantValues.add(Instant.ofEpochMilli(time));
                }
                Operator operator = determineOperator(ranges, instantValues);
                return new InstantPredicate(ArmorConstants.INTERVAL_START, operator, instantValues);
            } else {
                // Greater, less, between etc. NOTE: Equals not supported in presto on timestmp.
                Range range = ranges.iterator().next();                
                Operator operator = null;
                Marker highMarker = range.getHigh();
                Marker lowMarker = range.getLow();
                // For < and <=
                if (highMarker.getValueBlock().isPresent() && !lowMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE )
                        operator = Operator.LESS_THAN;
                    else if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.LESS_THAN_EQUAL;
                    long time = extractTimeValue(highMarker);
                    return new InstantPredicate(ArmorConstants.INTERVAL_START, operator, Arrays.asList(Instant.ofEpochMilli(time)));
                }
                // For > and >=
                if (lowMarker.getValueBlock().isPresent() && !highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.EXACTLY )
                        operator = Operator.GREATER_THAN_EQUAL;
                    else if (highMarker.getBound() == Bound.BELOW && lowMarker.getBound() == Bound.ABOVE)
                        operator = Operator.GREATER_THAN;                    
                    final long time = extractTimeValue(lowMarker);
                    return new InstantPredicate(ArmorConstants.INTERVAL_START, operator, Arrays.asList(Instant.ofEpochMilli(time)));
                }
                // For = or between
                if (highMarker.getValueBlock().isPresent() && highMarker.getValueBlock().isPresent()) {
                    if (highMarker.getBound() == Bound.EXACTLY && lowMarker.getBound() == Bound.EXACTLY) {
                       // Between and equal are the same only way to determine the difference is if the values are different.
                       long highValue = extractTimeValue(highMarker);
                       long lowValue = extractTimeValue(lowMarker);
                       if (highValue == lowValue) {
                         return new InstantPredicate(ArmorConstants.INTERVAL_START, Operator.EQUALS, Arrays.asList(Instant.ofEpochMilli(highValue)));
                       } else {
                         return new InstantPredicate(
                             ArmorConstants.INTERVAL_START,
                             Operator.BETWEEN,
                             Arrays.asList(Instant.ofEpochMilli(lowValue), Instant.ofEpochMilli(highValue)));
                       }
                    }
                }
            }
        }
        throw new RuntimeException("Unable to resolve predicate into armor predicate");
    }
    
    private static long extractTimeValue(Marker marker) {
        if (marker.getType() instanceof TimestampWithTimeZoneType) 
           return unpackMillisUtc((Long) marker.getValue());
        else
           return (Long) marker.getValue();
    }
}
