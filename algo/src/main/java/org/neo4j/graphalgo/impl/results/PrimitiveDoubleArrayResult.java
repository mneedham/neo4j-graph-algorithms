package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.results.CentralityResult;

public final class PrimitiveDoubleArrayResult implements CentralityResult {
    private final double[] result;

    public PrimitiveDoubleArrayResult(double[] result) {
        super();
        this.result = result;
    }

    @Override
    public double score(final int nodeId) {
        return result[nodeId];
    }

    @Override
    public double score(final long nodeId) {
        return score((int) nodeId);
    }

    @Override
    public void export(
            final String propertyName,
            final Exporter exporter) {
        exporter.write(propertyName, result, Translators.DOUBLE_ARRAY_TRANSLATOR);
    }
}
