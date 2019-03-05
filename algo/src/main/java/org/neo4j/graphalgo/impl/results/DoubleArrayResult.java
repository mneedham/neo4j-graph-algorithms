package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;

import java.util.Arrays;
import java.util.OptionalDouble;

public final class DoubleArrayResult implements CentralityResult {
    private final double[] result;
    private final double max;

    public DoubleArrayResult(double[] result) {
        this.result = result;
        this.max = Arrays.stream(result).parallel().max().orElse(1);
    }

    @Override
    public void export(
            final String propertyName, final Exporter exporter) {
        exporter.write(
                propertyName,
                result,
                Translators.DOUBLE_ARRAY_TRANSLATOR);
    }

    @Override
    public double max() {
        return max;
    }

    @Override
    public double l2Norm() {
        return 0;
    }

    @Override
    public double l1Norm() {
        return 0;
    }

    @Override
    public final double score(final long nodeId) {
        return result[(int) nodeId];
    }

    @Override
    public double score(final int nodeId) {
        return result[nodeId];
    }
}
