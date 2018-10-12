package org.neo4j.graphalgo.impl.infomap;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.Algorithm;

/**
 * @author mknblch
 */
public class MapEquation extends Algorithm<MapEquation> {

    private final Graph graph;
    private final ModuleTable table;

    public MapEquation(Graph graph) {
        this.graph = graph;
        table = new ModuleTable();
    }

    private void init() {

    }


    @Override
    public MapEquation me() {
        return this;
    }

    @Override
    public MapEquation release() {
        // TODO
        return this;
    }


}
