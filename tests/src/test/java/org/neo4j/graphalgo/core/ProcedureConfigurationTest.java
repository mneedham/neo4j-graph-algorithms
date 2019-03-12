package org.neo4j.graphalgo.core;

import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class ProcedureConfigurationTest {

    @Test
    public void useDefault() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("partitionProperty", "defaultValue");
        assertEquals(value, "defaultValue");
    }

    @Test
    public void returnValueIfPresent() {
        Map<String, Object> map = MapUtil.map("partitionProperty", "partition");
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("partitionProperty", "defaultValue");
        assertEquals(value, "partition");
    }

    @Test
    public void newKeyIfPresent() {
        Map<String, Object> map = MapUtil.map("partitionProperty", "old", "writeProperty", "new");
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals(value, "new");
    }

    @Test
    public void oldKeyIfNewKeyNotPresent() {
        Map<String, Object> map = MapUtil.map("partitionProperty", "old");
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals(value, "old");
    }

    @Test
    public void defaultIfNoKeysPresent() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = new ProcedureConfiguration(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals(value, "defaultValue");
    }
}