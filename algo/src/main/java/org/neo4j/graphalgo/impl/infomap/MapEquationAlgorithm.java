package org.neo4j.graphalgo.impl.infomap;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;

import java.util.stream.LongStream;

/**
 * @author mknblch
 */
public class MapEquationAlgorithm extends Algorithm<MapEquationAlgorithm> {

    public static final int ITERATIONS = 10;

    private final Graph graph;

    public MapEquationAlgorithm(Graph graph) {
        this.graph = graph;
    }

    public MapEquation build() {
        getProgressLogger().log("calculating pageRanks..");
        final PageRankResult result = PageRankAlgorithm.of(graph, 1. - MapEquation.TAU, LongStream.empty())
                .compute(ITERATIONS)
                .result();
        getProgressLogger().log("finished calculating pageRanks");
        return new MapEquation(graph, result::score);
    }

    public MapEquationLight light() {
        getProgressLogger().log("calculating pageRanks..");
        final PageRankResult result = PageRankAlgorithm.of(graph, 1. - MapEquation.TAU, LongStream.empty())
                .compute(ITERATIONS)
                .result();
        getProgressLogger().log("finished calculating pageRanks");
        return new MapEquationLight(graph, result::score);
    }

    @Override
    public MapEquationAlgorithm me() {
        return this;
    }

    @Override
    public MapEquationAlgorithm release() {
        return this;
    }
}
