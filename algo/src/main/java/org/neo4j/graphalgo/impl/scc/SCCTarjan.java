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
package org.neo4j.graphalgo.impl.scc;

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Sequential strongly connected components algorithm (Tarjan).
 * <p>
 * Builds sets of node-Ids which represent a strongly connected component
 * within the graph. Also calculates minimum and maximum setSize as well
 * as the overall count of distinct sets.
 */
public class SCCTarjan extends Algorithm<SCCTarjan> {

    private Graph graph;
    private final int nodeCount;
    private int[] communities;
    private int[] indices;
    private int[] lowLink;
    private final BitSet onStack;
    private final IntStack stack;
    private int index;


    public SCCTarjan(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        indices = new int[nodeCount];
        lowLink = new int[nodeCount];
        onStack = new BitSet(nodeCount);
        stack = new IntStack(nodeCount);
        communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
    }

    public SCCTarjan compute() {
        graph.forEachNode(this::test);
        return this;
    }

    public int[] getConnectedComponents() {
        return communities;
    }

    @Override
    public SCCTarjan me() {
        return this;
    }

    @Override
    public SCCTarjan release() {
        stack.clear();
        communities = null;
        indices = null;
        lowLink = null;
        graph = null;
        return this;
    }

    public void reset() {
        Arrays.fill(indices, -1);
        Arrays.fill(lowLink, -1);
        onStack.clear();
        stack.clear();
        index = 0;
    }

    private void strongConnect(int node) {
        lowLink[node] = index;
        indices[node] = index++;
        stack.push(node);
        onStack.set(node);
        graph.forEachRelationship(node, Direction.OUTGOING, this::accept);
        if (indices[node] == lowLink[node]) {
            relax(node);
        }
    }

    private void relax(int nodeId) {
        int w;
        do {
            w = stack.pop();
            onStack.clear(w);
            communities[w] = nodeId;
        } while (w != nodeId);
    }

    private boolean accept(int source, int target, long unused) {
        if (indices[target] == -1) {
            strongConnect(target);
            lowLink[source] = Math.min(lowLink[source], lowLink[target]);
        } else if (onStack.get(target)) {
            lowLink[source] = Math.min(lowLink[source], indices[target]);
        }
        return true;
    }

    private boolean test(int node) {
        if (indices[node] == -1) {
            strongConnect(node);
        }
        progressLogger.logProgress((double) node / (nodeCount - 1));
        return running();
    }
}
