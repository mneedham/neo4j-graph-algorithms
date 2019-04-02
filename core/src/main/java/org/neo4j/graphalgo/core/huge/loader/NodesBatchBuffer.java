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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.huge.loader.AbstractStorePageCacheScanner.RecordConsumer;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.StorageStatement.Nodes;


final class NodesBatchBuffer implements RecordConsumer<NodeRecord>, AutoCloseable {

    private final int label;
    private final RecordCursor<DynamicRecord> labelCursor;

    private int length;
    // node ids, consecutive
    private final long[] buffer;
    // property ids, consecutive
    private final long[] properties;

    NodesBatchBuffer(final Nodes store, final int label, int capacity, boolean readProperty) {
        this.label = label;
        this.labelCursor = label != Read.ANY_LABEL ? store.newLabelCursor() : null;
        this.buffer = new long[capacity];
        this.properties = readProperty ? new long[capacity] : null;
    }

    boolean scan(AbstractStorePageCacheScanner<NodeRecord>.Cursor cursor) {
        length = 0;
        return cursor.bulkNext(this) && length > 0;
    }

    @Override
    public void add(final NodeRecord record) {
        if (hasCorrectLabel(record)) {
            int len = length++;
            buffer[len] = record.getId();
            if (properties != null) {
                properties[len] = record.getNextProp();
            }
        }
    }

    // TODO: something label scan store
    private boolean hasCorrectLabel(final NodeRecord record) {
        if (label == Read.ANY_LABEL) {
            return true;
        }
        long labelField = record.getLabelField();

        final long[] labels;
        if (NodeLabelsField.fieldPointsToDynamicRecordOfLabels(labelField)) {
            labels = DynamicNodeLabels.get(record, labelCursor);
        } else {
            labels = InlineNodeLabels.get(record);
        }
        long label = (long) this.label;
        for (long l : labels) {
            if (l == label) {
                return true;
            }
        }
        return false;
    }

    int length() {
        return length;
    }

    long[] batch() {
        return buffer;
    }

    long[] properties() {
        return properties;
    }

    @Override
    public void close() {
        if (labelCursor != null) {
            labelCursor.close();
        }
    }
}
