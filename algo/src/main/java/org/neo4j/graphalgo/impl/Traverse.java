/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;

/**
 * @author mknblch
 */
public class Traverse extends Algorithm<Traverse> {

    private final int nodeCount;
    private Graph graph;
    private IntArrayDeque nodes;
    private IntArrayDeque sources;
    private DoubleArrayDeque weights;
    private BitSet visited;

    public Traverse(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        nodes = new IntArrayDeque(nodeCount);
        sources = new IntArrayDeque(nodeCount);
        weights = new DoubleArrayDeque(nodeCount);
        visited = new BitSet(nodeCount);
    }

    /**
     * start BFS without aggregator
     * @param sourceId source node id
     * @param direction traversal direction
     * @param exitCondition
     * @return
     */
    public long[] computeBfs(long sourceId, Direction direction, ExitPredicate exitCondition) {
        return traverse(graph.toMappedNodeId(sourceId), direction, exitCondition, (s, t, w) -> .0, IntArrayDeque::addLast, DoubleArrayDeque::addLast);
    }

    /**
     * start DSF without aggregator
     * @param sourceId source node id
     * @param direction traversal direction
     * @param exitCondition
     * @return
     */
    public long[] computeDfs(long sourceId, Direction direction, ExitPredicate exitCondition) {
        return traverse(graph.toMappedNodeId(sourceId), direction, exitCondition, (s, t, w) -> .0, IntArrayDeque::addFirst, DoubleArrayDeque::addFirst);
    }

    /**
     * start BFS using an aggregator function
     * @param sourceId source node id
     * @param direction traversal direction
     * @param exitCondition
     * @param aggregator
     * @return
     */
    public long[] computeBfs(long sourceId, Direction direction, ExitPredicate exitCondition, Aggregator aggregator) {
        return traverse(graph.toMappedNodeId(sourceId), direction, exitCondition, aggregator, IntArrayDeque::addLast, DoubleArrayDeque::addLast);
    }

    /**
     * start DFS using an aggregator function
     * @param sourceId source node id
     * @param direction traversal direction
     * @param exitCondition
     * @param aggregator
     * @return
     */
    public long[] computeDfs(long sourceId, Direction direction, ExitPredicate exitCondition, Aggregator aggregator) {
        return traverse(graph.toMappedNodeId(sourceId), direction, exitCondition, aggregator, IntArrayDeque::addFirst, DoubleArrayDeque::addFirst);
    }

    /**
     * traverse along the path
     * @param sourceNode source node (mapped id)
     * @param direction the traversal direction
     * @param exitCondition exit condition
     * @param agg weight accumulator function
     * @param nodeFunc node accessor function (either ::addLast or ::addFirst to switch between fifo and lifo behaviour)
     * @param weightFunc weight accessor function (either ::addLast or ::addFirst to switch between fifo and lifo behaviour)
     * @return a list of nodes that have been visited
     */
    private long[] traverse(int sourceNode,
                            Direction direction,
                            ExitPredicate exitCondition,
                            Aggregator agg,
                            ObjIntConsumer<IntArrayDeque> nodeFunc,
                            ObjDoubleConsumer<DoubleArrayDeque> weightFunc) {

        final LongArrayList list = new LongArrayList(nodeCount);
        nodes.clear();
        sources.clear();
        visited.clear();
        nodeFunc.accept(nodes, sourceNode);
        nodeFunc.accept(sources, sourceNode);
        weightFunc.accept(weights, .0);
        visited.set(sourceNode);
        loop:
        while (!nodes.isEmpty() && running()) {
            final int source = sources.removeFirst();
            final int node = nodes.removeFirst();
            final double weight = weights.removeFirst();
            switch (exitCondition.test(source, node, weight)) {
                case BREAK:
                    list.add(graph.toOriginalNodeId(node));
                    break loop;
                case CONTINUE:
                    continue loop;
                case FOLLOW:
                    list.add(graph.toOriginalNodeId(node));
                    break;
            }
            graph.forEachRelationship(
                    node,
                    direction, (s, t, relId) -> {
                        if (!visited.get(t)) {
                            visited.set(t);
                            nodeFunc.accept(sources, node);
                            nodeFunc.accept(nodes, t);
                            weightFunc.accept(weights, agg.apply(s, t, weight));
                        }
                        return running();
                    });
        }
        return list.toArray();
    }

    @Override
    public Traverse me() {
        return this;
    }

    @Override
    public Traverse release() {
        nodes = null;
        weights = null;
        visited = null;
        return this;
    }

    public interface ExitPredicate {

        enum Result {
            /**
             * add current node to the result set and visit all neighbors
             */
            FOLLOW,
            /**
             * add current node to the result set and terminate traversal
             */
            BREAK,
            /**
             * does not add node to the result set, does not follow its neighbors,
             * just continue with next element on the stack
             */
            CONTINUE
        }

        /**
         * called once for each accepted node during traversal
         *
         * @param sourceNode the source node
         * @param currentNode the current node
         * @param weightAtSource the total weight that has been collected by the Aggregator during the traversal
         * @return a result
         */
        Result test(int sourceNode, int currentNode, double weightAtSource);
    }


    public interface Aggregator {

        /**
         * aggregate weight between source and current node
         * @param sourceNode source node
         * @param currentNode the current node
         * @param weightAtSource the weight that has been aggregated for the currentNode so far
         * @return new weight (e.g. weightAtSource + 1.)
         */
        double apply(int sourceNode, int currentNode, double weightAtSource);
    }
}
