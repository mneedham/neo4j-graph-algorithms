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

import static org.neo4j.graphalgo.impl.infomap.MapEquationAlgorithm.entropy;

/**
 * @author mknblch
 */
public class MapEquationLight {

    private static final double LOG2 = Math.log(2);
    public static final double TAU = .15; // taken from pageRank paper

    private final NodeWeights pageRanks;
    private final ObjectArrayList<Module> modules;
    private final int[] communities;
    private final int nodeCount;
    private final double totalGraphPageRankEntropy;

    public MapEquationLight(Graph graph, NodeWeights pageRanks) {
        this.pageRanks = pageRanks;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.modules = new ObjectArrayList<>(nodeCount);
        this.communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
        final double[] d = {0.};
        graph.forEachNode(node -> {
            d[0] += entropy(pageRanks.weightOf(node));
            modules.add(new Module(node));
            return true;
        });
        this.totalGraphPageRankEntropy = d[0];
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
        for (Iterator<ObjectCursor<Module>> it = this.modules.iterator(); it.hasNext(); ) {
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

    public void forEachModule(Consumer<Module> moduleConsumer) {
        for (ObjectCursor<Module> module : modules) {
            if (module.value.nodes.isEmpty()) {
                continue;
            }
            moduleConsumer.accept(module.value);
        }
    }

    public double getIndexCodeLength() {
        if (modules.size() == 1) {
            return 0;
        }
        final double[] v = {0., 0.}; // {totalQout, entropy}
        forEachModule(m -> v[0] += m.qOut());
        forEachModule(m -> v[1] += entropy(m.qOut() / v[0]));
        System.out.println("Arrays.toString(v) = " + Arrays.toString(v));
        return v[0] * -v[1];
    }

    public double getModuleCodeLength() {
        final Pointer.DoublePointer p = Pointer.wrap(0.);
        forEachModule(m -> p.v += m.getCodeBookLength());
        return p.v;
    }

    public class Module {
        private final IntSet nodes;
        private double modulePageRank;

        private Module(int startNode) {
            this.nodes = new IntScatterSet();
            this.nodes.add(startNode);
            this.modulePageRank = pageRanks.weightOf(startNode);
        }

        public void add(int node) {
            this.nodes.add(node);
            this.modulePageRank += pageRanks.weightOf(node);
        }

        public void remove(int node) {
            if (nodes.removeAll(node) == 0) {
                return;
            }
            this.modulePageRank -= pageRanks.weightOf(node);
        }

        double qOut() {
            return TAU * modulePageRank * (1. - ((double) nodes.size() / nodeCount));
        }

        double qp() {
            return qOut() * modulePageRank;
        }

        public double getCodeBookLength() {
            if (nodes.size() == 0) {
                return 0;
            }
            final double qp = qOut() + modulePageRank;
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