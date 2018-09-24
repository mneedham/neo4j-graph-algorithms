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
package org.neo4j.graphalgo.similarity;

import info.debatty.java.lsh.LSHSuperBit;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.neo4jview.DirectIdMapping;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class BucketProc extends SimilarityProc {

    @Context
    public GraphDatabaseAPI dbAPI;

    @Procedure(name = "algo.similarity.bucket.stream", mode = Mode.READ)
    @Description("CALL algo.similarity.bucket.stream([{source:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD node1, bucket - computes cosine distance")
    public Stream<BucketResult> bucketStream(
            @Name(value = "data", defaultValue = "null") List<Map<String,Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        WeightedInput[] inputs = prepareWeights(data, getDegreeCutoff(configuration));

        int n = inputs[0].weights.length;

        long buckets = configuration.get("buckets", 10L);

        LSHSuperBit lsh = new LSHSuperBit(1, (int) buckets, n);

        return Stream.of(inputs).map(input -> {
            int[] hash = lsh.hash(input.weights);
            return new BucketResult(input.id, hash[0]);
        });
    }

    @Procedure(name = "algo.similarity.bucket", mode = Mode.WRITE)
    @Description("CALL algo.similarity.cosine([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD p50, p75, p90, p99, p999, p100 - computes cosine similarities")
    public Stream<BucketSummaryResult> bucket(
            @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        WeightedInput[] inputs = prepareWeights(data, getDegreeCutoff(configuration));

        int n = inputs[0].weights.length;

        long buckets = configuration.get("buckets", 10L);

        LSHSuperBit lsh = new LSHSuperBit(1, (int) buckets, n);

        IdMap idMapping = new IdMap(inputs.length);
        Stream<BucketResult> bucketStream = Stream.of(inputs).peek(item -> idMapping.add(item.id)).map(input -> {
            int[] hash = lsh.hash(input.weights);
            return new BucketResult(input.id, hash[0]);
        });

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        boolean shouldWrite = configuration.isWriteFlag(true);
        String propertyName = configuration.getWriteProperty("bucket");
        if (shouldWrite) {

            int[] labels = bucketStream.map(item ->item.bucket).mapToInt(Math::toIntExact).toArray();
            idMapping.buildMappedIds();

            Exporter.of(idMapping, dbAPI)
                    .withLog(log)
                    .parallel(Pools.DEFAULT, configuration.getConcurrency(), terminationFlag)
                    .build()
                    .write(propertyName,
                            labels,
                            Translators.INT_ARRAY_TRANSLATOR
                    );

        }

        return Stream.of(new BucketSummaryResult(inputs.length, shouldWrite, propertyName));
    }


}
