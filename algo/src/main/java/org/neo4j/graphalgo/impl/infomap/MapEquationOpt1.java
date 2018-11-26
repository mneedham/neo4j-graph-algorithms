package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.sources.ShuffledNodeIterator;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public class MapEquationOpt1 extends Algorithm<MapEquationOpt1> implements MapEquationAlgorithm {

    public static final double TAU = .15; // from pageRank paper
    private static final double LOG2 = Math.log(2);

    private final NodeWeights pageRanks;
    private final int nodeCount;
    private final RelationshipWeights weights;
    private final double totalGraphPageRankEntropy;
    private final Graph graph;
    private Direction direction = Direction.OUTGOING;

    private ObjectArrayList<Module> modules;
    private int[] communities;
    private int iterations = 0;

    public MapEquationOpt1(Graph graph, NodeWeights pageRanks, RelationshipWeights normalizedWeights) {
        this.graph = graph;
        this.pageRanks = pageRanks;
        this.weights = normalizedWeights;
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

    public MapEquationOpt1 withDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    public void move(int node, int community) {
        final int current = communities[node];
        this.communities[node] = community;
        this.modules.get(current).remove(node);
        this.modules.get(community).add(node);

    }

    public int getIterations() {
        return iterations;
    }

    public int[] getCommunities() {
        return communities;
    }

    public MapEquationOpt1 compute(int iterations, boolean shuffled) {
        final ProgressLogger progressLogger = getProgressLogger();
        for (this.iterations = 0; this.iterations < iterations; this.iterations++) {
            progressLogger.log("Iteration " + this.iterations);
            final PrimitiveIntIterator it = shuffled ?
                    new ShuffledNodeIterator(nodeCount).nodeIterator() :
                    graph.nodeIterator();
            if (!optimize(it)) {
                break;
            }
        }
        return this;
    }

    public double delta(int node, int community) {
        final int p = communities[node];
        move(node, community);
        final double mdl = getMDL();
        move(node, p);
        return mdl;
    }

    public double getMDL() {
        double totalQout = .0;
        double totalQoutE = .0;
        double totalE = .0;
        for (Iterator<ObjectCursor<Module>> it = this.modules.iterator(); it.hasNext(); ) {
            final Module mod = it.next().value;
            if (mod.nodes.isEmpty()) {
                continue;
            }
            final double qOut = mod.qOut();
            totalQout += qOut;
            totalQoutE += entropy(qOut);
            totalE += entropy(mod.qP());
        }
        return entropy(totalQout)
                - 2 * totalQoutE
                - totalGraphPageRankEntropy
                + totalE;
    }

    public double getIndexCodeLength() {
        if (modules.size() == 1) {
            return 0;
        }
        final double[] v = {0., 0.}; // {totalQout, entropy}
        forEachModule(m -> v[0] += m.qOut());
        forEachModule(m -> v[1] += entropy(m.qOut() / v[0]));
        return v[0] * -v[1];
    }

    @Override
    public int getCommunityCount() {
        return modules.size();
    }

    public int getNodeCount(int moduleIndex) {
        return modules.get(moduleIndex).nodes.size();
    }

    public double getModuleCodeLength() {
        final Pointer.DoublePointer p = Pointer.wrap(0.);
        forEachModule(m -> p.v += m.getCodeBookLength());
        return p.v;
    }

    @Override
    public MapEquationOpt1 me() {
        return this;
    }

    @Override
    public MapEquationOpt1 release() {
        modules.clear();
        modules = null;
        communities = null;
        return this;
    }

    private boolean optimize(PrimitiveIntIterator it) {
        boolean change = false;
        int t = 0;
        for (; it.hasNext(); ) {
            final int node = it.next();
            final Pointer.DoublePointer bestMdl = Pointer.wrap(getMDL());
            final Pointer.IntPointer bestCommunity = Pointer.wrap(communities[node]);
            forEachNeighboringCommunity(node, community -> {
                final double v = delta(node, community);
                if (v < bestMdl.v && bestMdl.v > .0) {
                    bestMdl.v = v;
                    bestCommunity.v = community;
                }
            });
            if (communities[node] != bestCommunity.v) {
                move(node, bestCommunity.v);
                change |= true;
            }
            getProgressLogger().logProgress(t++, nodeCount);
        }
        return change;
    }

    private void forEachNeighboringCommunity(int node, IntConsumer communityConsumer) {
        final BitSet set = new BitSet();
        graph.forEachRelationship(node, direction, (s, t, r) -> {
            final int targetCommunity = communities[t];
            if (set.get(targetCommunity)) {
                return true;
            }
            set.set(targetCommunity);
            communityConsumer.accept(targetCommunity);
            return true;
        });
    }

    private void forEachModule(Consumer<Module> moduleConsumer) {
        for (ObjectCursor<Module> module : modules) {
            if (module.value.nodes.isEmpty()) {
                continue;
            }
            moduleConsumer.accept(module.value);
        }
    }

    class Module {

        private final IntSet nodes;
        private double modulePageRank;
        private double qOut = 0;

        private Module(int startNode) {
            this.nodes = new IntScatterSet();
            this.nodes.add(startNode);

            this.modulePageRank = pageRanks.weightOf(startNode);

            qOut = q(startNode);
        }

        public void add(int node) {
            if (!this.nodes.add(node)) {
                return;
            }

            this.qOut += q(node);
            this.modulePageRank += pageRanks.weightOf(node);

        }

        public void remove(int node) {
            if (this.nodes.removeAll(node) == 0) {
                return;
            }
            this.qOut -= q(node);
            this.modulePageRank -= pageRanks.weightOf(node);
        }

        private double q(int node) {

            final double prSourceTau = pageRanks.weightOf(node) * (1. - TAU);

            // disconnected nodes
            if (graph.degree(node, Direction.OUTGOING) == 0) {
                return prSourceTau * (1. - ((double) nodes.size() / nodeCount));
            }
            // rest
            final Pointer.DoublePointer p = Pointer.wrap(.0);
            graph.forEachRelationship(node, direction, (sourceNodeId, targetNodeId, relationId) -> {
                // count weights into different communities
//                if (nodes.contains(targetNodeId)) {
//                    return true;
//                }
                if (communities[targetNodeId] == communities[sourceNodeId]) return true;
                p.v += prSourceTau * weights.weightOf(sourceNodeId, targetNodeId) ;
                return true;
            });
            return (p.v);
        }

        double qOut() {

            double k = 0;
            for (IntCursor node : nodes) {
                k += q(node.value);
            }

            System.out.println("qOut = " + qOut + " k = " + k + " d = " + Math.abs(qOut - k));
//
//            System.out.println("modulePageRank = " + modulePageRank);
//            System.out.println("m = " + m);
            return modulePageRank * TAU * (1. - (double) nodes.size() / nodeCount)
                    + k;
        }

        double qP() {
            return qOut() + modulePageRank;
        }

        double getCodeBookLength() {
            final double qOut = qOut();
            final double qp = qOut + modulePageRank;
            double e = 0;
            for (IntCursor node : nodes) {
                e += entropy(pageRanks.weightOf(node.value) / qp);
            }
            return qp * (-entropy(qOut / qp) - e);
        }

    }


    private static double entropy(double v) {
        return v != .0 ? v * log2(v) : 0.;
    }

    private static double log2(double v) {
        return Math.log(v) / LOG2;
    }
}