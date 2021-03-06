/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.neo4jview.DirectIdMapping;
import org.neo4j.graphalgo.core.sources.BothRelationshipAdapter;
import org.neo4j.graphalgo.core.sources.BufferedWeightMap;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.container.RelationshipContainer;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.MSTPrim;
import org.neo4j.graphalgo.results.MSTPrimResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class MSTPrimProc {

    public static final String CONFIG_WRITE_RELATIONSHIP = "writeProperty";
    public static final String CONFIG_WRITE_RELATIONSHIP_DEFAULT = "mst";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.mst", mode = Mode.WRITE)
    @Description("CALL algo.mst(node:Node, weightProperty:String, {nodeQuery:String, relationshipQuery:String, " +
            "write:boolean, writeProperty:String stats:boolean}) " +
            "YIELD loadMillis, computeMillis, writeMillis, weightSum, weightMin, weightMax, relationshipCount")
    public Stream<MSTPrimResult> mst(
            @Name("startNode") Node startNode,
            @Name(value = "weightProperty") String weightProperty,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        LazyIdMapper idMapper = LazyIdMapper.importer(api)
                .withWeightsFromProperty(weightProperty, 1.0)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .build();

        MSTPrimResult.Builder builder = MSTPrimResult.builder();

        BufferedWeightMap weightMap;
        RelationshipContainer relationshipContainer;

        int startNodeId = idMapper.toMappedNodeId(startNode.getId());

        try (ProgressTimer timer = builder.timeLoad()) {
            weightMap = BufferedWeightMap.importer(api)
                    .withIdMapping(idMapper)
                    .withAnyDirection(true)
                    .withWeightsFromProperty(weightProperty, 1.0)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .build();
            relationshipContainer = RelationshipContainer.importer(api)
                    .withIdMapping(idMapper)
                    .withDirection(Direction.BOTH)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .build();
        }

        final MSTPrim mstPrim = new MSTPrim(
                idMapper,
                new BothRelationshipAdapter(relationshipContainer),
                weightMap)
                .withProgressLogger(ProgressLogger.wrap(log, "MST(Prim)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(() -> {
            mstPrim.compute(startNodeId);
            if (configuration.isStatsFlag()) {
                MSTPrim.MinimumSpanningTree.Aggregator aggregator =
                        mstPrim.getMinimumSpanningTree().aggregate();
                builder.withWeightMax(aggregator.getMax())
                        .withWeightMin(aggregator.getMin())
                        .withWeightSum(aggregator.getSum())
                        .withRelationshipCount(aggregator.getCount());
            }
        });

        if (configuration.isWriteFlag()) {
            final MSTPrim.MinimumSpanningTree minimumSpanningTree = mstPrim.getMinimumSpanningTree();
            mstPrim.release();
            weightMap = null;
            relationshipContainer = null;
            builder.timeWrite(() -> {
                Exporter.of(new DirectIdMapping(1), api)
                        .withLog(log)
                        .build()
                        .writeRelationships(
                                configuration.get(CONFIG_WRITE_RELATIONSHIP, CONFIG_WRITE_RELATIONSHIP_DEFAULT),
                                (ops, typeId) -> minimumSpanningTree.forEachBFS(writeBack((int) typeId, idMapper, ops))
                        );
            });
        }

        return Stream.of(builder.build());
    }

    private static RelationshipConsumer writeBack(int typeId, IdMapping mapping, DataWriteOperations ops) {
        return (source, target, rid) -> {
            try {
                ops.relationshipCreate(
                        typeId,
                        mapping.toOriginalNodeId(source),
                        mapping.toOriginalNodeId(target)
                );
            } catch (KernelException e) {
                throw Exceptions.launderedException(e);
            }
            return true;
        };
    }
}
