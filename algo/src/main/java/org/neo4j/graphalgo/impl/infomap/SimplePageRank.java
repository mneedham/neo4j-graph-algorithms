package org.neo4j.graphalgo.impl.infomap;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

/**
 * @author mknblch
 */
public class SimplePageRank implements NodeWeights {

    public static final Direction D = Direction.OUTGOING;
    private final Graph graph;
    private final double[] pageRanks;
    private final int nodeCount;
    private final double dampingFactor;

    public SimplePageRank(Graph graph, double dampingFactor) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.dampingFactor = dampingFactor;
        pageRanks = new double[nodeCount];
        Arrays.fill(pageRanks, 1. / nodeCount);
    }

    public SimplePageRank compute(int iterations) {
        final double[] ref = new double[nodeCount];
        for (int i = 0; i < iterations; i++) {
            for (int n = 0; n < nodeCount; n++) {
                ref[n] = .0;
                graph.forEachOutgoing(n, (s, t, r) -> {
                    ref[s] += pageRanks[t] / graph.degree(t, D);
                    return true;
                });
            }
        }
        System.arraycopy(ref, 0, pageRanks, 0, nodeCount);
        return this;
    }

    public double[] getPageRanks() {
        return pageRanks;
    }

    @Override
    public double weightOf(int nodeId) {
        return pageRanks[nodeId];
    }
}
