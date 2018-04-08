/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.presto.kudu;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.kudu.client.KuduClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class KuduMetadata
        implements ConnectorMetadata
{
    public static final Logger logger = Logger.get(KuduMetadata.class);

    public static final String PRESTO_KUDU_SCHEMA = "default";
    private static final List<String> SCHEMA_NAMES = ImmutableList.of(PRESTO_KUDU_SCHEMA);

    private final KuduTables kuduTables;
    private final KuduClientManager kuduClientManager;

    @Inject
    public KuduMetadata(KuduTables kuduTables, KuduClientManager kuduClientManager)
    {
        this.kuduClientManager = kuduClientManager;
        this.kuduTables = requireNonNull(kuduTables, "kuduTables is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMA_NAMES;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        KuduClient kuduClient = kuduClientManager.getClient();
        ConnectorTableHandle connectorTableHandle = kuduTables.getTables(kuduClient).get(tableName);
        kuduClientManager.close(kuduClient);
        return connectorTableHandle;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        KuduClient kuduClient = kuduClientManager.getClient();
        KuduTableHandle tableHandle = Types.checkType(table, KuduTableHandle.class, "tableHandle");
        ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(
                tableHandle.getSchemaTableName(), kuduTables.getColumns(kuduClient, tableHandle));
        kuduClientManager.close(kuduClient);
        return connectorTableMetadata;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        KuduClient kuduClient = kuduClientManager.getClient();

        List<SchemaTableName> schemaTableNames = kuduTables.getTables(kuduClient)
                .keySet()
                .stream()
                .collect(Collectors.toList());

        kuduClientManager.close(kuduClient);
        return schemaTableNames;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        KuduTableHandle tableHandle = Types.checkType(table, KuduTableHandle.class, "tableHandle");
        ConnectorTableLayout layout = new ConnectorTableLayout(new KuduTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        KuduTableLayoutHandle layout = Types.checkType(handle, KuduTableLayoutHandle.class, "layout");
        return new ConnectorTableLayout(layout);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        KuduTableHandle tableHandle = Types.checkType(table, KuduTableHandle.class, "tableHandle");
        return getColumnHandles(tableHandle);
    }

    private Map<String, ColumnHandle> getColumnHandles(KuduTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        KuduClient kuduClient = kuduClientManager.getClient();

        for (ColumnMetadata column : kuduTables.getColumns(kuduClient, tableHandle)) {
            int ordinalPosition;
            ordinalPosition = index;
            index++;
            columnHandles.put(column.getName(), new KuduColumnHandle(column.getName(), column.getType(), ordinalPosition));
        }
        kuduClientManager.close(kuduClient);
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        Types.checkType(tableHandle, KuduTableHandle.class, "tableHandle");
        return Types.checkType(columnHandle, KuduColumnHandle.class, "columnHandle").toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        KuduClient kuduClient = kuduClientManager.getClient();

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            KuduTableHandle tableHandle = kuduTables.getTables(kuduClient).get(tableName);
            if (tableHandle != null) {
                columns.put(tableName, kuduTables.getColumns(kuduClient, tableHandle));
            }
        }
        kuduClientManager.close(kuduClient);
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KuduClient kuduClient = kuduClientManager.getClient();
        kuduClientManager.dropTable(kuduClient, ((KuduTableHandle) tableHandle).getSchemaTableName().getTableName());
        kuduClientManager.close(kuduClient);
    }
}
