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
package org.neo4j.graphalgo.impl.degree;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.pagerank.DegreeCentralityAlgorithm;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.PrimitiveDoubleArrayResult;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DegreeCentrality extends Algorithm<DegreeCentrality> implements DegreeCentralityAlgorithm {
    private final int nodeCount;
    private Direction direction;
    private int batchSize;
    private Graph graph;
    private final ExecutorService executor;
    private final int concurrency;
    private volatile AtomicInteger nodeQueue = new AtomicInteger();
    private double[] degrees;

    public DegreeCentrality(
            Graph graph,
            ExecutorService executor,
            int concurrency,
            Direction direction,
            int batchSize) {

        this.graph = graph;
        this.executor = executor;
        this.concurrency = concurrency;
        this.direction = direction;
        this.batchSize = batchSize;
        nodeCount = Math.toIntExact(graph.nodeCount());
        degrees = new double[nodeCount];
    }

    public void compute() {
        nodeQueue.set(0);

        int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, this.batchSize);
        int taskCount = ParallelUtil.threadSize(batchSize, nodeCount);
        final ArrayList<DegreeTask> tasks = new ArrayList<>(taskCount);

        for (int i = 0; i < taskCount; i++) {
            tasks.add(new DegreeTask());
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);
    }

    @Override
    public Algorithm<?> algorithm() {
        return this;
    }

    @Override
    public DegreeCentrality me() {
        return this;
    }

    @Override
    public DegreeCentrality release() {
        graph = null;
        return null;
    }

    @Override
    public CentralityResult result() {
        return new PrimitiveDoubleArrayResult(degrees);
    }

    private class DegreeTask implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                int degree = graph.degree(nodeId, direction);
                degrees[nodeId] = degree;

            }
        }
    }

    public double[] degrees() {
        return degrees;
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId ->
                        new Result(graph.toOriginalNodeId(nodeId), degrees[nodeId]));
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public final long nodeId;
        public final double centrality;

        public Result(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", centrality=" + centrality +
                    '}';
        }
    }

}
