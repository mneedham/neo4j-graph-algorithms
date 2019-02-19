package org.neo4j.graphalgo.similarity;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SimilarityProcTest {

    private final int concurrency;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Integer> data() {
        return Arrays.asList(
                1
        );
    }

    public SimilarityProcTest(int concurrency) {
        this.concurrency = concurrency;
    }

    public static final SimilarityProc.SimilarityComputer<CategoricalInput> ALL_PAIRS_COMPUTER = (decoder, source, target, cutoff) ->
            similarityResult(source.id, target.id, true, false);

    private static SimilarityResult similarityResult(long sourceId, long targetId, boolean birectional, boolean reversed) {
        return new SimilarityResult(sourceId, targetId, -1, -1, -1, 0.7, birectional, reversed);
    }

    public static final SimilarityProc.SimilarityComputer<CategoricalInput> COMPUTER = (decoder, source, target, cutoff) ->
            similarityResult(source.id, target.id, false,false);


    public static final Supplier<RleDecoder> DECODER = () -> null;

    @Test
    public void allPairs() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[3];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));
        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, ALL_PAIRS_COMPUTER, configuration, () -> null, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());
        assertEquals(3, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 1, true, false)));
        assertThat(rows, hasItems(similarityResult(0, 2, true, false)));
        assertThat(rows, hasItems(similarityResult(1, 2, true, false)));
    }

    @Test
    public void allPairsTopK() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[3];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));
        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, ALL_PAIRS_COMPUTER, configuration, () -> null, -1.0, 1);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());
        assertEquals(3, rows.size());

        for (SimilarityResult row : rows) {
            System.out.println(row);
        }

        assertThat(rows, hasItems(similarityResult(0, 1, true, false)));
        assertThat(rows, hasItems(similarityResult(1, 0,true, true)));
        assertThat(rows, hasItems(similarityResult(2, 0,true, true)));
    }

    @Test
    public void sourceSpecifiedTargetSpecified() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[3];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));
        int[] sourceIndexIds = new int[]{0};
        int[] targetIndexIds = new int[]{1, 2};
        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIndexIds, targetIndexIds, COMPUTER, configuration, DECODER, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());
        assertEquals(2, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 1, false, false)));
        assertThat(rows, hasItems(similarityResult(0, 2, false, false)));
    }

    @Test
    public void sourceSpecifiedTargetSpecifiedTopK() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[3];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));
        int[] sourceIndexIds = new int[]{0};
        int[] targetIndexIds = new int[]{1, 2};
        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIndexIds, targetIndexIds, COMPUTER, configuration, DECODER, -1.0, 1);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());
        assertEquals(1, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 1, false, false)));
    }

    @Test
    public void sourceSpecifiedTargetNotSpecified() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[4];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});
        ids[3] = new CategoricalInput(3, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));

        int[] sourceIndexIds = new int[]{0, 1};
        int[] targetIndexIds = new int[]{};

        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIndexIds, targetIndexIds, COMPUTER, configuration, DECODER, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());

        assertEquals(5, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 1, false, false)));
        assertThat(rows, hasItems(similarityResult(0, 2, false, false)));
        assertThat(rows, hasItems(similarityResult(0, 3, false, false)));
        assertThat(rows, hasItems(similarityResult(1, 2, false, false)));
        assertThat(rows, hasItems(similarityResult(1, 3, false, false)));
    }

    @Test
    public void sourceSpecifiedTargetNotSpecifiedTopK() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[4];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});
        ids[3] = new CategoricalInput(3, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));

        int[] sourceIndexIds = new int[]{0, 1};
        int[] targetIndexIds = new int[]{};

        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIndexIds, targetIndexIds, COMPUTER, configuration, DECODER, -1.0, 1);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());

        assertEquals(2, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 1, false, false)));
        assertThat(rows, hasItems(similarityResult(1, 2, false, false)));
    }

    @Test
    public void sourceNotSpecifiedTargetSpecified() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[4];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});
        ids[3] = new CategoricalInput(3, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));

        int[] sourceIndexIds = new int[]{};
        int[] targetIndexIds = new int[]{2,3};

        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIndexIds, targetIndexIds, COMPUTER, configuration, DECODER, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());

        assertEquals(6, rows.size());

        assertThat(rows, hasItems(similarityResult(0, 3, false, false)));
        assertThat(rows, hasItems(similarityResult(1, 3, false, false)));
        assertThat(rows, hasItems(similarityResult(2, 3, false, false)));

        assertThat(rows, hasItems(similarityResult(0, 2, false, false)));
        assertThat(rows, hasItems(similarityResult(1, 2, false, false)));
        assertThat(rows, hasItems(similarityResult(2, 3, false, false)));
    }

    @Test
    public void sourceNotSpecifiedTargetSpecifiedTopK() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[4];
        ids[0] = new CategoricalInput(0, new long[]{});
        ids[1] = new CategoricalInput(1, new long[]{});
        ids[2] = new CategoricalInput(2, new long[]{});
        ids[3] = new CategoricalInput(3, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));

        int[] sourceIndexIds = new int[]{};
        int[] targetIndexIds = new int[]{2,3};

        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIndexIds, targetIndexIds, COMPUTER, configuration, DECODER, -1.0, 1);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());

        assertEquals(4, rows.size());
        assertThat(rows, hasItems(similarityResult(0, 2, false, false)));
        assertThat(rows, hasItems(similarityResult(1, 2, false, false)));
        assertThat(rows, hasItems(similarityResult(2, 3, false, false)));
        assertThat(rows, hasItems(similarityResult(3, 2, false, false)));
    }

    @Test
    public void sourceTargetOverlap() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[4];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));

        int[] sourceIndexIds = new int[]{0,1,2};
        int[] targetIndexIds = new int[]{1,2};

        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIndexIds, targetIndexIds, COMPUTER, configuration, DECODER, -1.0, 0);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());

        assertEquals(4, rows.size());

        assertThat(rows, hasItems(similarityResult(5, 6, false, false)));
        assertThat(rows, hasItems(similarityResult(5, 7, false, false)));
        assertThat(rows, hasItems(similarityResult(6, 7, false, false)));
        assertThat(rows, hasItems(similarityResult(7, 6, false, false)));
    }

    @Test
    public void sourceTargetOverlapTopK() {
        SimilarityProc similarityProc = new SimilarityProc();

        CategoricalInput[] ids = new CategoricalInput[4];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});

        ProcedureConfiguration configuration = ProcedureConfiguration.create(MapUtil.map("concurrency", concurrency));

        int[] sourceIndexIds = new int[]{0,1,2};
        int[] targetIndexIds = new int[]{1,2};

        Stream<SimilarityResult> stream = similarityProc.similarityStream(ids, sourceIndexIds, targetIndexIds, COMPUTER, configuration, DECODER, -1.0, 1);

        List<SimilarityResult> rows = stream.collect(Collectors.toList());

        assertEquals(3, rows.size());
        assertThat(rows, hasItems(similarityResult(5, 6, false, false)));
        assertThat(rows, hasItems(similarityResult(6, 7, false, false)));
        assertThat(rows, hasItems(similarityResult(7, 6, false, false)));
    }

}