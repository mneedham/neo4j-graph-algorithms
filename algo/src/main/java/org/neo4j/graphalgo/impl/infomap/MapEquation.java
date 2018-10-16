package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;

/**
 * @author mknblch
 */
public class MapEquation {

    public static final double TAU = .15;
    public static final double LOG2 = Math.log(2);

    private final NodeWeights pageRanks;
    private final ObjectArrayList<Module> modules;
    private final int[] communities;
    private final int nodeCount;

    public MapEquation(Graph graph, NodeWeights pageRanks) {
        this.pageRanks = pageRanks;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.modules = new ObjectArrayList<>(nodeCount);
        this.communities = new int[nodeCount];
        graph.forEachNode(node -> {
            modules.add(new Module(node));
            return true;
        });
    }

    public void move(int node, int community) {
        final int current = communities[node];
        final Module module = modules.get(current);
        if (module.nodes.size() <= 1) {
            // rm this module since it is empty
            modules.remove(current);
        } else {
            // rm node from module
            module.remove(node);
        }
        // move node into new community
        this.modules.get(community).add(node);
        this.communities[node] = community;
    }

    private class Module {

        private final IntSet nodes;
        private double totalPageRank;

        private Module(int startNode) {
            this.nodes = new IntScatterSet();
            this.nodes.add(startNode);
            this.totalPageRank = pageRanks.weightOf(startNode);
        }

        public void add(int node) {
            this.nodes.add(node);
            this.totalPageRank += pageRanks.weightOf(node);
        }

        public void remove(int node) {
            if (nodes.removeAll(node) == 0) {
                return;
            }
            this.totalPageRank -= pageRanks.weightOf(node);
        }

        public double qOut() {
            return TAU * totalPageRank * 1. - ((double) nodes.size() / nodeCount);
        }

        public double getCodeBookLength() {
            final double qp = qOut() + totalPageRank;
            double e = 0;
            for (IntCursor node : nodes) {
                e += entropy(pageRanks.weightOf(node.value) / qp);
            }
            return qp * (-entropy(qOut() / qp) - e);
        }
    }

    private static double entropy(double v) {
        return v != .0 ? v * log2(v) : 0.;
    }

    private static double log2(double v) {
        return Math.log10(v) / LOG2;
    }

}
