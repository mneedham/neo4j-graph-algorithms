package org.neo4j.graphalgo.impl.infomap;

/**
 * @author mknblch
 */
public interface MapEquationAlgorithm {


    int[] getCommunities();

    double getMDL();

    double getIndexCodeLength();

    int getCommunityCount();

    double getModuleCodeLength();

    int getIterations();
}
