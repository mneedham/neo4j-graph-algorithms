package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.core.utils.Pointer;

import java.util.Arrays;
import java.util.Iterator;

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

    private final double totalGraphPageRankEntropy;

    public MapEquation(Graph graph, NodeWeights pageRanks) {
        this.pageRanks = pageRanks;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.modules = new ObjectArrayList<>(nodeCount);
        this.communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
        final Pointer.DoublePointer totalGraphPRE = Pointer.wrap(0.);
        graph.forEachNode(node -> {
            totalGraphPRE.v += entropy(pageRanks.weightOf(node));
            modules.add(new Module(node));
            return true;
        });
        this.totalGraphPageRankEntropy = totalGraphPRE.v;
    }

    public void move(int node, int community) {
        final int current = communities[node];
        this.modules.get(current).remove(node);
        this.modules.get(community).add(node);
        this.communities[node] = community;
    }

    public double getMDL() {

        double totalE = 0;
        double totalQout = 0;
        double totalQoutE = 0;

        for(Iterator<ObjectCursor<Module>> it = this.modules.iterator(); it.hasNext(); ) {
            final Module mod = it.next().value;
            if (mod.nodes.isEmpty()) {
                continue;
            }
            final double qout = mod.qOut();
            totalQout += qout;
            totalQoutE += entropy(qout);
            totalE += entropy(mod.qp());
        }

        return entropy(totalQout) -
                2 * totalQoutE -
                totalGraphPageRankEntropy +
                totalE;
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

        private double qp() {
            return qOut() * totalPageRank;
        }

        public double getCodeBookLength() {
            if (nodes.size() == 0) {
                return 0;
            }
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
        return Math.log(v) / LOG2;
    }

}
