package org.neo4j.graphalgo.impl.infomap;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.logging.Log;

import java.util.stream.LongStream;

/**
 * @author mknblch
 */
public class MapEquationAlgorithm extends Algorithm<MapEquationAlgorithm> {

    private static final double LOG2 = Math.log(2);

    public static final double TAU = .15;

    public static final int ITERATIONS = 10;

    private final Graph graph;

    public MapEquationAlgorithm(Graph graph) {
        this.graph = graph;
    }

    public MapEquationAlgorithm compute() {

        final MapEquationOptimization opt = opt();
        final long nodeCount = graph.nodeCount();

        /*
            TODO iterate (in rnd order?) over all neighboring communities of each node and assign
            the node to the community which leads to the best mdl
        */
        return this;
    }

    // TODO rm custom factory methods

    public MapEquation build() {
        getProgressLogger().log("calculating pageRanks..");
        final PageRankResult result = PageRankAlgorithm.of(graph, 1. - TAU, LongStream.empty())
                .compute(ITERATIONS)
                .result();
        getProgressLogger().log("finished calculating pageRanks");
        return new MapEquation(graph, result::score);
    }

    public MapEquationLight light() {
        getProgressLogger().log("calculating pageRanks..");
        final PageRankResult result = PageRankAlgorithm.of(graph, 1. - TAU, LongStream.empty())
                .compute(ITERATIONS)
                .result();
        getProgressLogger().log("finished calculating pageRanks");
        return new MapEquationLight(graph, result::score);
    }

    public MapEquationOptimization opt() {
        getProgressLogger().log("calculating pageRanks..");
        final PageRankResult result = PageRankAlgorithm.of(graph, 1. - TAU, LongStream.empty())
                .compute(ITERATIONS)
                .result();
        getProgressLogger().log("finished calculating pageRanks");
        return new MapEquationOptimization(graph, result::score);
    }

    @Override
    public MapEquationAlgorithm me() {
        return this;
    }

    @Override
    public MapEquationAlgorithm release() {
        return this;
    }

    public static double entropy(double v) {
        return v != .0 ? v * log2(v) : 0.;
    }

    public static double log2(double v) {
        return Math.log(v) / LOG2;
    }
}
