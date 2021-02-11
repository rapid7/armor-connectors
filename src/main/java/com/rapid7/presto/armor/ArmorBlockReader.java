package com.rapid7.presto.armor;


import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

import java.util.Map;
import java.util.Optional;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.common.type.Type;
import com.rapid7.armor.read.fast.FastArmorBlock;
import com.rapid7.armor.read.fast.FastArmorBlockReader;

public class ArmorBlockReader {

    private Map<String, FastArmorBlockReader> columnReaders;
    private final int DEFAULT_BATCH_SIZE = 500000;
    private int batchSize = DEFAULT_BATCH_SIZE;
    public ArmorBlockReader(Map<String, FastArmorBlockReader> columnReaders) {
        this.columnReaders = columnReaders;
        this.batchSize = DEFAULT_BATCH_SIZE;
    }
    
    public int batchSize() {
        for (FastArmorBlockReader far : columnReaders.values()) {
            if (far.hasNext()) {
                return far.nextBatchSize(batchSize);
            }
        }
        return -1;
    }

    // Reads in a block of a given array.
    public Block read(Type type, String name) {
        FastArmorBlockReader reader = columnReaders.get(name);
        if (type.equals(VARCHAR)) {
            FastArmorBlock ab = reader.getStringBlock(reader.nextBatchSize(batchSize));
            return new VariableWidthBlock(ab.getValuesIsNull().length, ab.getSlice(), ab.getOffsets(), Optional.of(ab.getValuesIsNull()));
        }
        else if (type.equals(INTEGER)) {
            FastArmorBlock ab = reader.getIntegerBlock(reader.nextBatchSize(batchSize));
            Optional<boolean[]> nullOption = ab.getValuesIsNull() == null || ab.getValuesIsNull().length == 0 ? Optional.empty() : Optional.of(ab.getValuesIsNull());
            return new IntArrayBlock(ab.getIntValueArray().length, nullOption, ab.getIntValueArray());
        }
        else if (type.equals(BIGINT)) {
            FastArmorBlock ab = reader.getLongBlock(reader.nextBatchSize(batchSize));
            Optional<boolean[]> nullOption = ab.getValuesIsNull() == null || ab.getValuesIsNull().length == 0 ? Optional.empty() : Optional.of(ab.getValuesIsNull());
            return new LongArrayBlock(ab.getLongValueArray().length, nullOption, ab.getLongValueArray());
        }
        else
            throw new UnsupportedOperationException("Type not supported: " + type);
    }
}
