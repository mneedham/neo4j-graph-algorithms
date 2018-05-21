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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.core.*;
import org.neo4j.graphalgo.core.huge.HugeIdMap;
import org.neo4j.graphalgo.core.huge.HugeNodeImporter;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLoggerAdapter;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The Abstract Factory defines the construction of the graph
 *
 * @author mknblch
 */
public abstract class GraphFactory {

    public static final String TASK_LOADING = "LOADING";

    protected final ExecutorService threadPool;
    protected final GraphDatabaseAPI api;
    protected final GraphSetup setup;
    protected final GraphDimensions dimensions;
    protected final ImportProgress progress;

    protected Log log = NullLog.getInstance();
    protected ProgressLogger progressLogger = ProgressLogger.NULL_LOGGER;

    public GraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        this.threadPool = setup.executor;
        this.api = api;
        this.setup = setup;
        this.log = setup.log;
        this.progressLogger = progressLogger(log, setup.logMillis, TimeUnit.MILLISECONDS);
        dimensions = new GraphDimensions(api, setup).call();
        progress = new ImportProgress(
                progressLogger,
                setup.tracker,
                dimensions.hugeNodeCount(),
                dimensions.maxRelCount(),
                setup.loadIncoming,
                setup.loadOutgoing);
    }

    public abstract Graph build();

    protected IdMap loadIdMap() throws EntityNotFoundException {
        final NodeImporter nodeImporter = new NodeImporter(
                api,
                progress,
                dimensions.nodeCount(),
                dimensions.labelId());
        return nodeImporter.call();
    }

    protected HugeIdMap loadHugeIdMap(AllocationTracker tracker) throws EntityNotFoundException {
        final HugeNodeImporter nodeImporter = new HugeNodeImporter(
                api,
                tracker,
                progress,
                dimensions.hugeNodeCount(),
                dimensions.allNodesCount(),
                dimensions.labelId());
        return nodeImporter.call();
    }

    protected WeightMapping newWeightMap(int propertyId, double defaultValue) {
        return propertyId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullWeightMap(defaultValue)
                : new WeightMap(dimensions.nodeCount(), defaultValue, propertyId);
    }

    protected VectorMapping newVectorMap(int propertyId, double[] defaultValue) {
        return propertyId == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new NullVectorMap(defaultValue)
                : new VectorMap(dimensions.nodeCount(), defaultValue, propertyId);
    }

    protected HugeWeightMapping hugeWeightMapping(
            AllocationTracker tracker,
            int propertyId,
            double defaultValue) {
        return propertyId == StatementConstants.NO_SUCH_PROPERTY_KEY
                    ? new HugeNullWeightMap(defaultValue)
                    : new HugeWeightMap(dimensions.hugeNodeCount(), defaultValue, tracker);
    }

    private static ProgressLogger progressLogger(Log log, long time, TimeUnit unit) {
        if (log == NullLog.getInstance()) {
            return ProgressLogger.NULL_LOGGER;
        }
        ProgressLoggerAdapter logger = new ProgressLoggerAdapter(log, TASK_LOADING);
        if (time > 0) {
            logger.withLogIntervalMillis((int) Math.min(unit.toMillis(time), Integer.MAX_VALUE));
        }
        return logger;
    }
}
