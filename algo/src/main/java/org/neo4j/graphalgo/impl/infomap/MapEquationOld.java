package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.core.sources.ShuffledNodeIterator;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.impl.Algorithm;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public class MapEquationOld extends Algorithm<MapEquationOld> implements MapEquationAlgorithm {

    private static final double LOG2 = Math.log(2);
    public static final double TAU = .15; // taken from pageRank paper

    private final Graph graph;
    // known module indices
    private final IntSet modules;
    // precalculated weight map containing page ranks
    private final NodeWeights pageRanks;
    private final int nodeCount;
    // sum(entropy(pageRank(node_i)))
    private double totalGraphPageRankEntropy;
    // community structure
    private int[] communities;
    // sum of pageRanks of all nodes of a community
    private double[] modulePageRank;
    // number of nodes in a community
    private int[] nodes;
    // helper variables
    private double totalQ = 0;
    private double totalQEntropy = 0;
    private double totalQPEntropy = 0;

    public MapEquationOld(Graph graph, NodeWeights pageRanks) {
        this.graph = graph;
        this.pageRanks = pageRanks;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.communities = new int[nodeCount];
        this.nodes = new int[nodeCount];
        this.modulePageRank = new double[nodeCount];
        Arrays.setAll(communities, i -> i);
        Arrays.fill(nodes, 1);
        Arrays.setAll(modulePageRank, pageRanks::weightOf);
        modules = new IntScatterSet(nodeCount);
        // precalculate helper vars
        for (int node = 0; node < nodeCount; node++) {
            modules.add(node);
            this.totalGraphPageRankEntropy += entropy(pageRanks.weightOf(node));
            this.totalQ += qOut(node);
            this.totalQEntropy += entropy(qOut(node));
            this.totalQPEntropy += entropy(qp(node));
        }

    }

    public MapEquationOld compute(int iterations, boolean shuffled) {
        double mdl = getMDL();
        for (int i = 0; i < iterations; i++) {
            final PrimitiveIntIterator it = shuffled ? new ShuffledNodeIterator(nodeCount).nodeIterator() :
                    graph.nodeIterator();
            for (; it.hasNext(); ) {
                final int node = it.next();
                final Pointer.DoublePointer bestMdl = Pointer.wrap(0.) ; //deltaMDL(node, communities[node]));
                final Pointer.IntPointer bestCommunity = Pointer.wrap(communities[node]);
                forEachCommunity(node, community -> {
                    final double v = deltaMDL(node, community);
                    if (v > bestMdl.v) {
                        bestMdl.v = v;
                        bestCommunity.v = community;
                    }
                });
                if (communities[node] != bestCommunity.v) {
                    move(node, bestCommunity.v);
                }
            }
            final double nmdl = getMDL();
            if (nmdl >= mdl) { // || Double.isNaN(mdl)) {
                break;
            }
            mdl = nmdl;
        }
        return this;
    }

    @Override
    public int[] getCommunities() {
        return communities;
    }

    @Override
    public double getMDL() {
        return entropy(totalQ)
                - 2 * totalQEntropy
                - totalGraphPageRankEntropy
                + totalQPEntropy;
    }

    @Override
    public double getIndexCodeLength() {
        if (getCommunityCount() <= 1) {
            return 0;
        }
        final double totalQOut = totalQOut();
        double v = 0;
        for (Iterator<IntCursor> it = modules.iterator(); it.hasNext(); ) {
            v += entropy(qOut(it.next().value) / totalQOut);

        }
        return totalQOut * -v;
    }

    @Override
    public int getCommunityCount() {
        return modules.size();
    }

    @Override
    public double getModuleCodeLength() {
        return 0;
    }

    @Override
    public int getIterations() {
        return 0;
    }

    public int getNodeCount(int moduleIndex) {
        return nodes[moduleIndex];
    }

    @Override
    public MapEquationOld me() {
        return this;
    }

    @Override
    public MapEquationOld release() {
        communities = null;
        modulePageRank = null;
        nodes = null;
        return this;
    }

    public void move(int node, int community) {
        final int c = communities[node];
        if (c == community) {
            return;
        }
        final double v = pageRanks.weightOf(node);
        totalQ -= qOut(c);
        totalQEntropy -= entropy(qOut(c));
        totalQPEntropy -= entropy(qp(c));
        if (nodes[c]-- <= 0) {
            modules.removeAll(c);
        }
        nodes[community]++;
        modulePageRank[c] -= v;
        modulePageRank[community] += v;
        totalQ += qOut(node);
        totalQEntropy += entropy(qOut(node));
        totalQPEntropy += entropy(qp(node));
        this.communities[node] = community;
    }

    private void forEachCommunity(int node, IntConsumer communityConsumer) {
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

    private double deltaMDL(int node, int community) {
        final double npr = modulePageRank[community] + pageRanks.weightOf(node);
        final double e = TAU * npr * (1. - ((double) (nodes[community] + 1) / nodeCount));
        return entropy(totalQ - qOut(communities[node]) + e)
                - 2 * (totalQEntropy - entropy(qOut(communities[node]))
                + entropy(e))
                - totalGraphPageRankEntropy
                + totalQPEntropy
                - entropy(qp(communities[node]))
                + entropy(e * npr);
    }

    private double totalQOut() {
        double qOut = 0.;
        for (Iterator<IntCursor> it = modules.iterator(); it.hasNext(); ) {
            qOut += qOut(it.next().value);
        }
        return qOut;
    }

    private double qOut(int community) {
        return TAU * modulePageRank[community] * (1. - ((double) nodes[community] / nodeCount));
    }

    private double qp(int community) {
        return qOut(community) * modulePageRank[community];
    }

    private static double entropy(double v) {
        return v != .0 ? v * log2(v) : 0.;
    }

    private static double log2(double v) {
        return Math.log(v) / LOG2;
    }
}
