package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Result;

class RelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private long lastSourceId = -1, lastTargetId = -1;
    private int source = -1, target = -1;
    private long rows = 0;
    private IdMap idMap;
    private boolean hasRelationshipWeights;
    private WeightMap relWeights;
    private AdjacencyMatrix matrix;

    RelationshipRowVisitor(IdMap idMap, boolean hasRelationshipWeights, WeightMap relWeights, AdjacencyMatrix matrix) {
        this.idMap = idMap;
        this.hasRelationshipWeights = hasRelationshipWeights;
        this.relWeights = relWeights;
        this.matrix = matrix;
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        rows++;
        long sourceId = row.getNumber("source").longValue();
        if (sourceId != lastSourceId) {
            source = idMap.get(sourceId);
            lastSourceId = sourceId;
        }
        if (source == -1) {
            return true;
        }
        long targetId = row.getNumber("target").longValue();
        if (targetId != lastTargetId) {
            target = idMap.get(targetId);
            lastTargetId = targetId;
        }
        if (target == -1) {
            return true;
        }
        if (hasRelationshipWeights) {
            long relId = RawValues.combineIntInt(source, target);
            Object weight = getProperty(row, "weight");
            if (weight instanceof Number) {
                relWeights.put(relId, ((Number) weight).doubleValue());
            }
        }

//                if (!matrix.hasOutgoing(source, target)) {
        matrix.addOutgoing(source, target);
//                }

        return true;
    }

    private Object getProperty(Result.ResultRow row, String propertyName) {
        try {
            return row.get(propertyName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public long rows() {
        return rows;
    }
}
