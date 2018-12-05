package org.neo4j.graphalgo.impl.infomap;

/**
 * @author mknblch
 */
public interface MapEquationAlgorithm {

    double LOG2 = Math.log(2.);

    int[] getCommunities();

    double getMDL();

    double getIndexCodeLength();

    int getCommunityCount();

    double getModuleCodeLength();

    int getIterations();

    static double plogp(double v) {
        return v > .0 ? v * log2(v) : 0.;
//        return v != .0 ? v * log2(v) : 0.;
//        final double a = v >= .0 ? v * log2(v) : 0.;
//        return a;
//        System.out.println("a = " + a);
//        return Double.isNaN(a) ? 0. : a;
    }

    static double log2(double v) {
        return Math.log(v) / LOG2;
    }
}
