package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;

import java.util.Arrays;
import java.util.OptionalDouble;

public final class DoubleArrayResult implements CentralityResult {
    private final double[] result;

    public DoubleArrayResult(double[] result) {
        this.result = result;
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
    public double computeMax() {
        return Arrays.stream(result).parallel().max().orElse(1);
    }

    @Override
    public double computeL2Norm() {
        return Math.sqrt(Arrays.stream(result).parallel().map(value -> value * value).sum());
    }

    @Override
    public double computeL1Norm() {
        return Arrays.stream(result).parallel().sum();
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
