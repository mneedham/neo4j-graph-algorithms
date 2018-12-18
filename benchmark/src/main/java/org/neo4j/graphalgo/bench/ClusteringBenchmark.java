/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.infomap.InfoMap;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.LongStream;

/**
 * @author mknblch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ClusteringBenchmark {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    private static final int NODE_COUNT = 100;

    private Graph g;
    private GraphDatabaseAPI api;

    @Param({"0.1", "0.3", "0.6"})
    private double connectedness;

    @Param({"HEAVY"}) //, "HUGE"})
    GraphImpl graph;
    private PageRankResult pageRankResult;


    @Setup
    public void setup() {
        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms for " + NODE_COUNT + " nodes"))) {
            GraphBuilder.create(api)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newCompleteGraphBuilder()
                    .createCompleteGraph(NODE_COUNT, connectedness);
        }

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            g = new GraphLoader(api)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .withSort(true)
                    .asUndirected(true)
                    .load(graph.impl);

            pageRankResult = PageRankAlgorithm.of(g, 1. - InfoMap.TAU, LongStream.empty())
                    .compute(10)
                    .result();
        }

    }


    @TearDown
    public void tearDown() {
        if (api != null) api.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public Object _01_louvain() {
        return new Louvain(g, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(ProgressLogger.NULL_LOGGER)
                .compute(10, 99999)
                .getCommunityCount();
    }

    @Benchmark
    public Object _02_infoMap() {
        return InfoMap.unweighted(g, pageRankResult::score, InfoMap.THRESHOLD, InfoMap.TAU)
                .compute()
                .getCommunityCount();
    }
}
