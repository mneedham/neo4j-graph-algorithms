package org.neo4j.graphalgo.similarity;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.neo4j.graphalgo.similarity.CategoricalInput.extractInputIds;

public class CategoricalInputTest {

    @Test
    public void findOneItem() {
        CategoricalInput[] ids = new CategoricalInput[3];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});

        int[] indexes = CategoricalInput.indexes(extractInputIds(ids), Arrays.asList( 5L));

        assertArrayEquals(indexes, new int[] {0});
    }

    @Test
    public void findMultipleItems() {
        CategoricalInput[] ids = new CategoricalInput[5];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});
        ids[4] = new CategoricalInput(9, new long[]{});

        int[] indexes = CategoricalInput.indexes(extractInputIds(ids), Arrays.asList( 5L, 9L));

        assertArrayEquals(indexes, new int[] {0, 4});
    }

    @Test
    public void missingItem() {
        CategoricalInput[] ids = new CategoricalInput[5];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});
        ids[4] = new CategoricalInput(9, new long[]{});

        int[] indexes = CategoricalInput.indexes(extractInputIds(ids), Arrays.asList( 10L));

        assertArrayEquals(indexes, new int[] {});
    }

    @Test
    public void allMissing() {
        CategoricalInput[] ids = new CategoricalInput[5];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});
        ids[4] = new CategoricalInput(9, new long[]{});

        int[] indexes = CategoricalInput.indexes(extractInputIds(ids), Arrays.asList( 10L ,11L, -1L, 29L));

        assertArrayEquals(indexes, new int[] {});
    }

    @Test
    public void someMissingSomeFound() {
        CategoricalInput[] ids = new CategoricalInput[5];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});
        ids[4] = new CategoricalInput(9, new long[]{});

        int[] indexes = CategoricalInput.indexes(extractInputIds(ids), Arrays.asList( 10L ,5L, 7L, 12L));

        assertArrayEquals(indexes, new int[] {0, 2});
    }

}