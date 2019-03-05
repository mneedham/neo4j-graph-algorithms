package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public final class PartitionedPrimitiveDoubleArrayResult implements CentralityResult, PropertyTranslator.OfDouble<double[][]> {
    private final double[][] partitions;
    private final int[] starts;

    public PartitionedPrimitiveDoubleArrayResult(
            double[][] partitions,
            int[] starts) {
        this.partitions = partitions;
        this.starts = starts;
    }

    @Override
    public void export(
            final String propertyName,
            final Exporter exporter) {
        exporter.write(
                propertyName,
                partitions,
                this
        );
    }

    @Override
    public double toDouble(final double[][] data, final long nodeId) {
        int idx = binaryLookup((int) nodeId, starts);
        return data[idx][(int) (nodeId - starts[idx])];
    }

    @Override
    public double score(final int nodeId) {
        int idx = binaryLookup(nodeId, starts);
        return partitions[idx][nodeId - starts[idx]];
    }

    @Override
    public double score(final long nodeId) {
        return toDouble(partitions, nodeId);
    }
}
