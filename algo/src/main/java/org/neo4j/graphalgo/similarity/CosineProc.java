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

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.helpers.collection.MapUtil.map;

public class CosineProc extends SimilarityProc {

    @Procedure(name = "algo.similarity.cosine.stream", mode = Mode.READ)
    @Description("CALL algo.similarity.cosine.stream([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD item1, item2, count1, count2, intersection, similarity - computes cosine distance")
    // todo count1,count2 = could be the non-null values, intersection the values where both are non-null?
    public Stream<SimilarityResult> cosineStream(
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Double skipValue = configuration.get("skipValue", null);

        SimilarityComputer<WeightedInput> computer = skipValue == null ?
                (s,t,cutoff) -> s.cosineSquares(cutoff, t) :
                (s,t,cutoff) -> s.cosineSquaresSkip(cutoff, t, skipValue);

        if (ProcedureConstants.CYPHER_QUERY.equals(configuration.getGraphName("dense"))) {
            if (skipValue == null) {
                throw new IllegalArgumentException("Must specify 'skipValue' when using {graph: 'cypher'}");
            }

//            List<Map<String, Object>> data = buildMap(api, (String) rawData, configuration, skipValue);

            WeightedInput[] inputs = prepareWeights(api, (String) rawData, configuration.getParams(), getDegreeCutoff(configuration), skipValue);

            double similarityCutoff = getSimilarityCutoff(configuration);
            // as we don't compute the sqrt until the end
            if (similarityCutoff > 0d) similarityCutoff *= similarityCutoff;

            int topN = getTopN(configuration);
            int topK = getTopK(configuration);

            Stream<SimilarityResult> stream = topN(similarityStream(inputs, computer, configuration, similarityCutoff, topK), topN);

            return stream.map(SimilarityResult::squareRooted);
        } else {
            List<Map<String, Object>> data = (List<Map<String, Object>>) rawData;
            WeightedInput[] inputs = prepareWeights(data, getDegreeCutoff(configuration), skipValue);

            double similarityCutoff = getSimilarityCutoff(configuration);
            // as we don't compute the sqrt until the end
            if (similarityCutoff > 0d) similarityCutoff *= similarityCutoff;

            int topN = getTopN(configuration);
            int topK = getTopK(configuration);

            Stream<SimilarityResult> stream = topN(similarityStream(inputs, computer, configuration, similarityCutoff, topK), topN);

            return stream.map(SimilarityResult::squareRooted);
        }
    }

    private List<Map<String, Object>> buildMap(GraphDatabaseAPI api, String rawData, ProcedureConfiguration configuration, double skipValue) throws Exception {
        Result result = api.execute(rawData, configuration.getParams());
        List<Map<String,Object>> data = new ArrayList<>();
        Map<Object, LongDoubleMap> map = new HashMap<>();
        LongSet ids = new LongHashSet();
        result.accept((Result.ResultVisitor<Exception>) resultRow -> {
            Object item = resultRow.get("item");
            long id = resultRow.getNumber("id").longValue();
            ids.add(id);
            double weight = resultRow.getNumber("weight").doubleValue();
            map.compute(item, (key, agg) -> {
                if (agg == null) agg= new LongDoubleHashMap();
                agg.put(id, weight);
                return agg;
            });
            return true;
        });
        long[] idsArray = ids.toArray();
        map.forEach((k,v) -> {
            ArrayList<Number> list = new ArrayList<>(ids.size());
            for (long id : idsArray) {
                list.add(v.getOrDefault(id,skipValue))   ;
            }
            data.add(map("item", k, "weights", list));
        });
        return data;
    }

    @Procedure(name = "algo.similarity.cosine", mode = Mode.WRITE)
    @Description("CALL algo.similarity.cosine([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD p50, p75, p90, p99, p999, p100 - computes cosine similarities")
    public Stream<SimilaritySummaryResult> cosine(
            @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Double skipValue = configuration.get("skipValue", null);
        SimilarityComputer<WeightedInput> computer = skipValue == null ?
                (s,t,cutoff) -> s.cosineSquares(cutoff, t) :
                (s,t,cutoff) -> s.cosineSquaresSkip(cutoff, t, skipValue);

        WeightedInput[] inputs = prepareWeights(data, getDegreeCutoff(configuration), skipValue);

        double similarityCutoff = getSimilarityCutoff(configuration);
        // as we don't compute the sqrt until the end
        if (similarityCutoff > 0d) similarityCutoff *= similarityCutoff;

        int topN = getTopN(configuration);
        int topK = getTopK(configuration);

        Stream<SimilarityResult> stream = topN(similarityStream(inputs, computer, configuration, similarityCutoff, topK), topN)
                .map(SimilarityResult::squareRooted);


        boolean write = configuration.isWriteFlag(false) && similarityCutoff > 0.0;
        return writeAndAggregateResults(configuration, stream, inputs.length, write, "SIMILAR");
    }


}
