package org.neo4j.graphalgo.impl.infomap;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.function.IntFunction;
import java.util.function.IntToDoubleFunction;

/**
 * @author mknblch
 */
public class MapEquationV3 {

    public static final Direction D = Direction.OUTGOING;
    private final Graph graph;
    private final NodeWeights pageRanks;
    private final RelationshipWeights weights;
    private final int nodeCount;
    private final int[] communities;
    private final double totalGraphPageRankEntropy;

    public MapEquationV3(Graph graph, NodeWeights pageRanks, RelationshipWeights weights) {
        this.graph = graph;
        this.pageRanks = pageRanks;
        this.weights = weights;
        this.totalGraphPageRankEntropy = sumOverNodes(graph.nodeIterator(), pageRanks::weightOf);
        nodeCount = Math.toIntExact(graph.nodeCount());
        communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
    }

    private static double sumOverNodes(PrimitiveIntIterator iterator, IntToDoubleFunction weights) {
        double s = 0.;
        for (; iterator.hasNext();) {
            s += weights.applyAsDouble(iterator.next());
        }
        return s;
    }
}
