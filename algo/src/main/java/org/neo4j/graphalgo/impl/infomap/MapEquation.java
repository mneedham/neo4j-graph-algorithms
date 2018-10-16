package org.neo4j.graphalgo.impl.infomap;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.core.utils.Pointer;

import java.util.Iterator;

/**
 * @author mknblch
 */
public class MapEquation {

    public static final double TAU = .15;
    public static final double LOG2 = Math.log(2);

    private final Graph graph;
    private final NodeWeights pageRanks;
    private final ObjectArrayList<Module> modules;
    private final int[] communities;
    private final int nodeCount;

    public MapEquation(Graph graph, NodeWeights pageRanks) {
        this.graph = graph;
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
            module.nodes.removeAll(node);
        }
        // move node into new community
        this.modules.get(community).nodes.add(node);
    }

    private class Module {

        final IntSet nodes;

        // todo inline
        double totalPageRank;

        private Module(int startNode) {
            this.nodes = new IntScatterSet();
            this.nodes.add(startNode);

        }

        double pageRank() {
            double totalPr = 0.;
            for (IntCursor c : nodes) {
                totalPr += pageRanks.weightOf(c.value);
            }
            return totalPr;
        }


        double proportion() {
            return 1. - ((double) nodes.size() / graph.nodeCount());
        }

        double qOut(double totalPr) {
            return TAU * totalPr * proportion();
        }

        double codeBookLength() {
            final double totalPr = pageRank();
            final double qout = qOut(totalPr);
            final double qp = qout * totalPr;
            double e = 0;
            for(Iterator<IntCursor> i = nodes.iterator(); i.hasNext();) {
                e += entropy(pageRanks.weightOf(i.next().value) / qp);
            }
            return qp * (-entropy(qout / qp) - e);
        }
    }

    private static double entropy(double v) {
        return v != .0 ? v * log2(v) : 0.;
    }

    private static double log2(double v) {
        return Math.log10(v) / LOG2;
    }

}
