package org.neo4j.graphalgo.similarity;

import org.junit.Test;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;

public class SimilarityProcTest {

    public static final SimilarityProc.SimilarityComputer<CategoricalInput> COMPUTER = (decoder, source, target, cutoff) ->
            similarityResult(source.id, target.id);

    private static SimilarityResult similarityResult(long sourceId, long targetId) {
        return new SimilarityResult(sourceId, targetId, -1, -1, -1, 0.7);
    }

    public static final Supplier<RleDecoder> DECODER = () -> null;

    @Test
    public void allPairs() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[3];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", 1));
        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, COMPUTER, configuration, () -> null, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());
        assertEquals(3, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 1)));
        assertThat(rows, hasItems(similarityResult(0, 2)));
        assertThat(rows, hasItems(similarityResult(1, 2)));
    }

    @Test
    public void sourceSpecifiedTargetSpecified() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[3];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", 1));
        int[] sourceIds = new int[]{0};
        int[] targetIds = new int[]{1, 2};
        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIds, targetIds, COMPUTER, configuration, DECODER, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());
        assertEquals(2, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 1)));
        assertThat(rows, hasItems(similarityResult(0, 2)));
    }

    @Test
    public void sourceSpecifiedTargetNotSpecified() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[4];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});
        ids[3] = new CategoricalInput(3, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", 1));

        int[] sourceIds = new int[]{0, 1};
        int[] targetIds = new int[]{};

        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIds, targetIds, COMPUTER, configuration, DECODER, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());

        for (SimilarityResult row : rows) {
            System.out.println(row);
        }

        assertEquals(5, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 1)));
        assertThat(rows, hasItems(similarityResult(0, 2)));
        assertThat(rows, hasItems(similarityResult(0, 3)));
        assertThat(rows, hasItems(similarityResult(1, 2)));
        assertThat(rows, hasItems(similarityResult(1, 3)));
    }

    @Test
    public void sourceNotSpecifiedTargetSpecified() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[4];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});
        ids[3] = new CategoricalInput(3, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", 1));

        int[] sourceIds = new int[]{};
        int[] targetIds = new int[]{2,3};

        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIds, targetIds, COMPUTER, configuration, DECODER, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());

        for (SimilarityResult row : rows) {
            System.out.println(row);
        }

        assertEquals(6, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 3)));
        assertThat(rows, hasItems(similarityResult(1, 3)));
        assertThat(rows, hasItems(similarityResult(2, 3)));

        assertThat(rows, hasItems(similarityResult(0, 2)));
        assertThat(rows, hasItems(similarityResult(1, 2)));
        assertThat(rows, hasItems(similarityResult(2, 3)));
    }

    @Test
    public void sourceTargetOverlap() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[4];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});
        ids[3] = new CategoricalInput(3, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", 1));

        int[] sourceIds = new int[]{0,1,2};
        int[] targetIds = new int[]{1,2};

        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIds, targetIds, COMPUTER, configuration, DECODER, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());

        for (SimilarityResult row : rows) {
            System.out.println(row);
        }

        assertEquals(4, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 1)));
        assertThat(rows, hasItems(similarityResult(0, 2)));

        assertThat(rows, hasItems(similarityResult(1, 2)));

        assertThat(rows, hasItems(similarityResult(2, 1)));


    }

}