package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.write.Exporter;

import java.util.function.BiFunction;

public  class NormalizedCentralityResult implements CentralityResult {
    private CentralityResult result;
    private BiFunction<Double, CentralityResult, Double> normalizationFunction;

    public NormalizedCentralityResult(CentralityResult result, BiFunction<Double, CentralityResult, Double> normalizationFunction) {
        this.result = result;
        this.normalizationFunction = normalizationFunction;
    }

    @Override
    public void export(String propertyName, Exporter exporter) {
        result.export(propertyName, exporter);
    }

    @Override
    public double score(int nodeId) {
        return normalizationFunction.apply(result.score(nodeId), result);
    }

    @Override
    public double score(long nodeId) {
        return normalizationFunction.apply(result.score(nodeId), result);
    }

    @Override
    public double max() {
        return result.max();
    }

    @Override
    public double l2Norm() {
        return 0;
    }

    @Override
    public double l1Norm() {
        return 0;
    }
}
