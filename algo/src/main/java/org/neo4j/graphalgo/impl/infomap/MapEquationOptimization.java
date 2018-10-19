package org.neo4j.graphalgo.impl.infomap;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.core.sources.ShuffledNodeIterator;
import org.neo4j.graphalgo.core.utils.Pointer;

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

import static org.neo4j.graphalgo.impl.infomap.MapEquationAlgorithm.entropy;

/**
 * @author mknblch
 */
public class MapEquationOptimization {

    private final Graph graph;
    private final NodeWeights pageRanks;
    private final int nodeCount;
    private final double totalGraphPageRankEntropy;
    private final int[] communities;
    private final double[] modulePageRank;
    private final int[] nodes;
    private int moduleCount;
    private double totalQPEntropy = 0;
    private double totalQ = 0;
    private double totalQEntropy = 0;

    public MapEquationOptimization(Graph graph, NodeWeights pageRanks) {
        this.graph = graph;
        this.pageRanks = pageRanks;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.communities = new int[nodeCount];
        this.nodes = new int[nodeCount];
        this.modulePageRank = new double[nodeCount];
        Arrays.setAll(communities, i -> i);
        Arrays.fill(nodes, 1);
        Arrays.setAll(modulePageRank, pageRanks::weightOf);
        final double[] d = {0., 0., 0., 0.}; // e(pr_i), qOut, e(qOut), e(qp)
        graph.forEachNode(node -> {
            d[0] += entropy(pageRanks.weightOf(node));
            d[1] += qOut(node);
            d[2] += entropy(qOut(node));
            d[3] += entropy(qp(node));
            return true;
        });
        this.moduleCount = nodeCount;
        this.totalGraphPageRankEntropy = d[0];
        this.totalQ = d[1];
        this.totalQEntropy = d[2];
        this.totalQPEntropy = d[3];
    }

    public void compute() {

        final PrimitiveIntIterator it = new ShuffledNodeIterator(nodeCount).nodeIterator();

        for (; it.hasNext();) {
            final int node = it.next();
            final Pointer.DoublePointer bestMdl = Pointer.wrap(deltaMDL(node, communities[node]));
            final Pointer.IntPointer bestCommunity = Pointer.wrap(communities[node]);
            forEachCommunity(node, community -> {
                final double v = deltaMDL(node, community);
                if (v > bestMdl.v) {
                    bestMdl.v = v;
                    bestCommunity.v = community;
                }
            });
            System.out.println("node = " + node);
            System.out.println("bestCommunity = " + bestCommunity.v);
            System.out.println("bestMdl = " + bestMdl.v);
            move(node, bestCommunity.v);
            System.out.println(Arrays.toString(communities));
        }
    }

    public int[] getCommunities() {
        return communities;
    }

    public void forEachCommunity(int node, IntConsumer communityConsumer) {
        final BitSet set = new BitSet();
        graph.forEachOutgoing(node, (s, t, r) -> {
            final int targetCommunity = communities[t];
            if (set.get(targetCommunity)) {
                return true;
            }
            set.set(targetCommunity);
            communityConsumer.accept(targetCommunity);
            return true;
        });
    }


    public void move(int node, int community) {
        final int c = communities[node];
        if (c == community) {
            return;
        }
        final double v = pageRanks.weightOf(node);
        totalQ -= qOut(node);
        totalQEntropy -= entropy(qOut(node));
        totalQPEntropy -= entropy(qp(node));
        if (nodes[c]-- <= 0) {
            moduleCount--;
        }
        nodes[community]++;
        modulePageRank[c] -= v;
        modulePageRank[community] += v;
        totalQ += qOut(node);
        totalQEntropy += entropy(qOut(node));
        totalQPEntropy += entropy(qp(node));

        this.communities[node] = community;
    }

    public double getMDL() {
        return entropy(totalQ) -
                2 * totalQEntropy -
                totalGraphPageRankEntropy +
                totalQPEntropy;
    }

    double deltaMDL(int node, int community) {

        final double mdl0 = entropy(totalQ) -
                2 * totalQEntropy -
                totalGraphPageRankEntropy +
                totalQPEntropy;

        final double e = MapEquationAlgorithm.TAU * modulePageRank[community] * (1. - ((double) (nodes[node] + 1) / nodeCount));
        final double qp = e * modulePageRank[node];
        double _totalQ = totalQ - qOut(node);
        double _totalQEntropy = totalQEntropy - entropy(qOut(node));
        double _totalQPEntropy = totalQPEntropy - entropy(qp(node));
        _totalQ += e;
        _totalQEntropy += entropy(e);
        _totalQPEntropy += entropy(qp);
        return mdl0 - entropy(_totalQ) -
                2 * _totalQEntropy -
                totalGraphPageRankEntropy +
                _totalQPEntropy;
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

        final com.carrotsearch.hppc.BitSet set = new com.carrotsearch.hppc.BitSet();

        for (int i = 0; i < nodeCount; i++) {
            final int c = this.communities[i];
            if (set.get(c)) {
                continue;
            }
            set.set(c);
            qOut += qOut(c);
        }
        return qOut;
    }

    private double qOut(int community) {
        return MapEquationAlgorithm.TAU * modulePageRank[community] * (1. - ((double) nodes[community] / nodeCount));
    }

    private double qp(int node) {
        return qOut(node) * modulePageRank[node];
    }


}
