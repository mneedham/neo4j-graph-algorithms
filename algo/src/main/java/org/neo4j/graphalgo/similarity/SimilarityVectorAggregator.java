package org.neo4j.graphalgo.similarity;

import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimilarityVectorAggregator {
    private List<Map<String, Object>> vector = new ArrayList<>();

    @UserAggregationUpdate
    public void next(
            @Name("node") Node node, @Name("weight") double weight) {
        vector.add(MapUtil.map("id", node.getId(), "weight", weight));
    }

    @UserAggregationResult
    public List<Map<String, Object>> result() {
        return vector;
    }
}
