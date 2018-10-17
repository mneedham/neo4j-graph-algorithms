package org.neo4j.graphalgo.impl.infomap;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;

import java.util.Arrays;

/**
 * @author mknblch
 */
public class MapEquationOptimization {

    public static final double TAU = .15;
    public static final double LOG2 = Math.log(2);

    private final NodeWeights pageRanks;
    private final int nodeCount;
    private final double totalGraphPageRankEntropy;
    private final int[] communities;
    private final double[] modulePageRank;
    private final int[] nodes;
    private int moduleCount;


    private double _e = 0;
    private double _q = 0;
    private double _qe = 0;
    private double _mdl = 0;

    public MapEquationOptimization(Graph graph, NodeWeights pageRanks) {
        this.pageRanks = pageRanks;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.communities = new int[nodeCount];
        this.nodes = new int[nodeCount];
        this.modulePageRank = new double[nodeCount];
        Arrays.setAll(communities, i -> i);
        Arrays.fill(nodes, 1);
        Arrays.setAll(modulePageRank, pageRanks::weightOf);
        final double[] d = {0., 0., 0., 0., 0.}; // pr_i, e(pr_i), qOut, e(qOut), e(qp)
        graph.forEachNode(node -> {
            d[0] += pageRanks.weightOf(node);
            d[1] += entropy(pageRanks.weightOf(node));
            d[2] += qOut(node);
            d[3] += entropy(qOut(node));
            d[4] += entropy(qp(node));
            return true;
        });
        this.moduleCount = nodeCount;
        this.totalGraphPageRankEntropy = d[1];
        this._e = d[4];
        this._q = d[2];
        this._qe = d[3];
    }

    public void move(int node, int community) {
        final int c = communities[node];
        if (c == community) {
            return;
        }
        final double v = pageRanks.weightOf(node);
        _q -= qOut(node);
        _qe -= entropy(qOut(node));
        _e -= entropy(qp(node));
        if (nodes[c]-- <= 0) {
            moduleCount--;
        }
        nodes[community]++;
        modulePageRank[c] -= v;
        modulePageRank[community] += v;
        _q += qOut(node);
        _qe += entropy(qOut(node));
        _e += entropy(qp(node));

        this.communities[node] = community;
    }

    public double getMDL() {

        return entropy(_q) -
                2 * _qe -
                totalGraphPageRankEntropy +
                _e;
    }

//
//    double deltaMDL(int node, int community) {
//
//    }

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
