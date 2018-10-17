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
import java.util.function.Consumer;

/**
 * @author mknblch
 */
public class MapEquationLight {

    public static final double TAU = .15;
    public static final double LOG2 = Math.log(2);

    private final NodeWeights pageRanks;
    private final int nodeCount;
    private final double totalGraphPageRankEntropy;
    private final int[] communities;
    private final double[] modulePageRank;
    private final int[] nodes;
    private int moduleCount;

    public MapEquationLight(Graph graph, NodeWeights pageRanks) {
        this.pageRanks = pageRanks;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
        final double[] d = {0.};
        graph.forEachNode(node -> {
            d[0] += entropy(pageRanks.weightOf(node));
            return true;
        });
        this.moduleCount = nodeCount;
        this.totalGraphPageRankEntropy = d[0];
        this.nodes = new int[nodeCount];
        this.modulePageRank = new double[nodeCount];
        Arrays.fill(nodes, 1);
        Arrays.setAll(modulePageRank, pageRanks::weightOf);
    }

    public void move(int node, int community) {
        final int c = communities[node];
        if (c == community) {
            return;
        }
        final double v = pageRanks.weightOf(node);
        if (nodes[c]-- <= 0) {
            moduleCount--;
        }

        nodes[community]++;
        modulePageRank[c] -= v;
        modulePageRank[community] += v;
        this.communities[node] = community;
    }

    public double getMDL() {

        double totalE = 0;
        double totalQout = 0;
        double totalQoutE = 0;

        for (int i = 0; i < nodeCount; i++) {
            if (nodes[i] == 0) {
                continue;
            }
            final double qout = qOut(i);
            totalQout += qout;
            totalQoutE += entropy(qout);
            totalE += entropy(qp(i));
        }

        return entropy(totalQout) -
                2 * totalQoutE -
                totalGraphPageRankEntropy +
                totalE;
    }

    public double getIndexCodeLength() {
        if (moduleCount <= 1) {
            return 0;
        }
        final double[] v = {0., 0.}; // {totalQout, entropy}

        final double sumQout = qOut();

        for (int i = 0; i < nodeCount; i++) {
            v[0] += entropy(qOut(i) / sumQout);
        }
        return sumQout * -v[0];
    }


    private double qOut() {
        double qOut = 0;
        for (int i = 0; i < nodeCount; i++) {
            qOut += qOut(i);
        }
        return qOut;
    }

    private double qOut(int node) {
        return TAU * modulePageRank[node] * (1. - ((double) nodes[node] / nodeCount));
    }

    private double qp(int node) {
        return qOut(node) * modulePageRank[node];
    }

    private static double entropy(double v) {
        return v != .0 ? v * log2(v) : 0.;
    }

    private static double log2(double v) {
        return Math.log(v) / LOG2;
    }

}
