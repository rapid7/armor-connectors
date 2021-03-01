package com.rapid7.presto.armor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.predicate.Marker.Bound;
import com.rapid7.armor.read.predicate.InstantPredicate;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.store.Operator;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;

import io.airlift.slice.Slice;

public class ArmorDomainUtil {
    private static String valueToString(Object value) {
        if (value != null) {
            if (value instanceof Slice)
              return ((Slice)value).toStringUtf8();
            else if (value instanceof String)
              return (String) value;
        }
        return null;
    }
    
    public static StringPredicate intervalPredicate(Domain predicate) {
        ValueSet values = predicate.getValues();
        if (values instanceof SortedRangeSet) {
            SortedRangeSet srs = (SortedRangeSet) values;
            List<Range> ranges = srs.getOrderedRanges();
            if (ranges.size() > 1) {
                // In clause style predicate
                List<String> filters = new ArrayList<>();
                for (Range range : ranges) {
                   String interval = valueToString(range.getSingleValue());
                   filters.add(interval);
                }
                return new StringPredicate(ArmorConstants.INTERVAL, Operator.IN, filters);
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
                List<Instant> filters = new ArrayList<>();
                for (Range range : ranges) {
                   Long time = extractTimeValue(range.getLow()); // Range values are single set for low.
                   filters.add(Instant.ofEpochMilli(time));
                }
                return new InstantPredicate(ArmorConstants.INTERVAL_START, Operator.IN, filters);
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
