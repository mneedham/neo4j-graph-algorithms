package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.core.WeightMap;

import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.LIMIT;
import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.SKIP;

public class CypherLoadingUtils {
    public static  boolean canBatchLoad(boolean loadConcurrent, int batchSize, String statement) {
        return loadConcurrent && batchSize > 0 &&
                (statement.contains("{" + LIMIT + "}") || statement.contains("$" + LIMIT)) &&
                (statement.contains("{" + SKIP + "}") || statement.contains("$" + SKIP));
    }

    public static WeightMap newWeightMapping(boolean needWeights, double defaultValue, int capacity) {
        return needWeights ? new WeightMap(capacity, defaultValue, -2) : null;
    }
}
