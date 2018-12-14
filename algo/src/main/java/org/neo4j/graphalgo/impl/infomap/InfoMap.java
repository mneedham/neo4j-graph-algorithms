package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.procedures.IntDoubleProcedure;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.DegreeNormalizedRelationshipWeights;
import org.neo4j.graphalgo.core.utils.NormalizedRelationshipWeights;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.pagerank.PageRankResult;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.LongStream;

/**
 * Java adaption of InfoMap from https://github.com/felixfung/InfoFlow
 *
 * @author mknblch, Mark Schaarschmidt
 */
public class InfoMap extends Algorithm<InfoMap> {

    private static final double LOG2 = Math.log(2.);

    // default TAU
    public static final double TAU = 0.15;
    // default threshold
    public static final double THRESHOLD = .005;

    // iteration direction is constant (undirected graph)
    private static final Direction D = Direction.OUTGOING;
    // nodes in the graph
    private final int nodeCount;
    // constant TAU (0.15 was given in the original MapEq. paper https://arxiv.org/abs/0906.1405 )
    private final double tau;
    // absolute minimum difference in deltaL for merging 2 modules together
    private final double threshold;
    // an undirected graph
    private Graph graph;
    // page rank weights
    private NodeWeights pageRank;
    // normalized relationship weights
    private RelationshipWeights weights;

    // helper vars
    private final double tau1, n1;

    // following values are updated during a merge
    // number of iterations the last computation took
    private int iterations = 0;
    // module map
    private IntObjectMap<Module> modules;
    // community assignment helper array
    private int[] communities;
    // sum of exit probs.
    private double sQi = 0.;

    /**
     * create a weighted InfoMap algo instance
     */
    public static InfoMap weighted(Graph graph, int prIterations, RelationshipWeights weights, double threshold, double tau) {
        final PageRankResult pageRankResult = PageRankAlgorithm.of(graph, 1. - tau, LongStream.empty())
                .compute(prIterations)
                .result();
        return weighted(graph, pageRankResult::score, weights, threshold, tau);
    }

    /**
     * create a weighted InfoMap algo instance with pageRanks
     */
    public static InfoMap weighted(Graph graph, NodeWeights pageRanks, RelationshipWeights weights, double threshold, double tau) {
        return new InfoMap(
                graph,
                pageRanks,
                new NormalizedRelationshipWeights(Math.toIntExact(graph.nodeCount()), graph, weights),
                threshold,
                tau);
    }

    /**
     * create an unweighted InfoMap algo instance
     */
    public static InfoMap unweighted(Graph graph, int prIterations, double threshold, double tau) {
        final PageRankResult pageRankResult = PageRankAlgorithm.of(graph, 1. - tau, LongStream.empty())
                .compute(prIterations)
                .result();
        return unweighted(graph, pageRankResult::score, threshold, tau);
    }

    /**
     * create an unweighted InfoMap algo instance
     */
    public static InfoMap unweighted(Graph graph, NodeWeights pageRanks, double threshold, double tau) {
        return new InfoMap(
                graph,
                pageRanks,
                new DegreeNormalizedRelationshipWeights(graph),
                threshold,
                tau);
    }

    /**
     * @param graph             graph
     * @param pageRank         page ranks
     * @param normalizedWeights normalized weights (weights of a node must sum up to 1.0)
     * @param threshold         minimum delta L for optimization
     * @param tau               constant tau (usually 0.15)
     */
    private InfoMap(Graph graph, NodeWeights pageRank, RelationshipWeights normalizedWeights, double threshold, double tau) {
        this.graph = graph;
        this.pageRank = pageRank;
        this.weights = normalizedWeights;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.tau = tau;
        this.threshold = threshold;
        this.modules = new IntObjectScatterMap<>(nodeCount);
        this.communities = new int[nodeCount];
        this.tau1 = 1. - tau;
        this.n1 = nodeCount - 1.;
        Arrays.setAll(communities, i -> i);
        final double[] d = {0.};
        graph.forEachNode(node -> {
            final Module module = new Module(node);
            modules.put(node, module);
            d[0] += module.q;
            return true;
        });
        this.sQi = d[0];
    }

    /**
     * minimize description length
     */
    public InfoMap compute() {
        this.iterations = 0;
        while (optimize()) {
            this.iterations++;
        }
        return this;
    }

    @Override
    public InfoMap me() {
        return this;
    }

    /**
     * release data
     *
     * @return
     */
    @Override
    public InfoMap release() {
        graph = null;
        pageRank = null;
        weights = null;
        modules = null;
//        communities = null;
        return this;
    }

    /**
     * return community array
     *
     * @return
     */
    public int[] getCommunities() {
        return communities;
    }

    /**
     * number of distinct modules left
     *
     * @return number of modules
     */
    public int getCommunityCount() {
        return modules.size();
    }

    /**
     * return number of iterations
     *
     * @return number of iterations
     */
    public int getIterations() {
        return iterations;
    }

    /**
     * find a pair of modules that lead to the
     * best reduction in L and merge them as
     * long as their absolute difference is
     * higher then {@link InfoMap#threshold}
     *
     * @return true if a merge occurred, false otherwise
     */
    private boolean optimize() {
        final Pointer.DoublePointer m = Pointer.wrap(-1 * threshold);
        final int[] best = {-1, -1};
        for (IntObjectCursor<Module> cursor : modules) {
            final Module module = cursor.value;
            forEachNeighboringModule(module, other -> {
                final double v = delta(module, other);
                if (v < m.v) {
                    m.v = v;
                    best[0] = module.index;
                    best[1] = other.index;
                }
            });
        }
        // merge module with higher i into mod with lower i
        if (best[0] == -1 || best[0] == best[1]) {
            return false;
        }
        if (best[0] < best[1]) {
            modules.get(best[0]).merge(modules.remove(best[1]));
        } else {
            modules.get(best[1]).merge(modules.remove(best[0]));
        }
        return true;
    }

    /**
     * call consumer for each connected community once
     *
     * @param m        the module
     * @param consumer module consumer
     */
    private void forEachNeighboringModule(Module m, Consumer<Module> consumer) {

        final BitSet bitSet = new BitSet(nodeCount);

        m.wil.keys().forEach((IntProcedure) node -> {
            final int c = communities[node];
            if (c == m.index) {
                return;
            }
            // already visited
            if (bitSet.get(c)) {
                return;
            }
            // do visit
            bitSet.set(c);
            consumer.accept(modules.get(c));
        });

    }

    /**
     * change in L if module j and k are merged
     *
     * @param j module a
     * @param k module b
     * @return delta L
     */
    private double delta(Module j, Module k) {
        double ni = j.n + k.n;
        double pi = j.p + k.p;
        double wi = j.w + k.w - j.delta(k); //interModW(j, k);
        double qi = tau * pi * (nodeCount - ni) / n1 + tau1 * wi;
        return plogp(qi - j.q - k.q + sQi)
                - plogp(sQi)
                - 2 * plogp(qi)
                + 2 * plogp(j.q)
                + 2 * plogp(k.q)
                + plogp(pi + qi)
                - plogp(j.p + j.q)
                - plogp(k.p + k.q);
    }


    /**
     * a module represents a community
     */
    private class Module {
        // module id
        final int index;
        // set size (number of nodes)
        int n = 1;
        // nodes
        BitSet nodes;
        // ergodic frequency
        double p;
        // exit probability without teleport
        double w;
        // exit probability with teleport
        double q;
        // precalculated in and out weights
        IntDoubleMap wil = new IntDoubleScatterMap();

        public Module(int startNode) {
            nodes = new BitSet(nodeCount);
            this.index = startNode;
            nodes.set(startNode);
            final Pointer.DoublePointer sumW = Pointer.wrap(.0);
            // sum of all weights
            graph.forEachRelationship(startNode, D, (s, t, r) -> {
                if (s != t) {
                    final double v = weights.weightOf(s, t);
                    sumW.v += v;
                    wil.put(t, v * pageRank.weightOf(s) + weights.weightOf(t, s) * pageRank.weightOf(t));
                }
                return true;
            });
            p = pageRank.weightOf(startNode);
            w = p * sumW.v;
            q = tau * p + tau1 * w;
        }

        public double delta(Module other) {
            final Pointer.DoublePointer wi = Pointer.wrap(0.);
            wil.forEach((IntDoubleProcedure) (key, value) -> {
                if (communities[key] == other.index) {
                    wi.v += value ;
                }
            });
            return wi.v;
        }

        public void merge(Module other) {
            nodes.or(other.nodes);
            n += other.n;
            p += other.p;
            wil.putAll(other.wil);
            w += other.w - delta(other); //interModW(this, other);
            sQi -= q + other.q;
            q = tau * p * (nodeCount - n) / n1 + tau1 * w;
            sQi += q;
            other.nodes.asIntLookupContainer().forEach((IntProcedure) n -> communities[n] = index);
        }
    }

    private static double plogp(double v) {
        return v > .0 ? v * log2(v) : 0.;
    }

    private static double log2(double v) {
        return Math.log(v) / LOG2;
    }
}
