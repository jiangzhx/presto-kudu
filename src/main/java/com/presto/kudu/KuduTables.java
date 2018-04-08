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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.presto.kudu.KuduMetadata.PRESTO_KUDU_SCHEMA;

public class KuduTables
{
    public static final Logger logger = Logger.get(KuduTables.class);

    //    private Map<SchemaTableName, KuduTableHandle> tables;
    private KuduClientManager kuduClientManager = null;

    @Inject
    public KuduTables(KuduClientManager kuduClientManager)
    {
        this.kuduClientManager = kuduClientManager;
    }

//    public KuduTableHandle getTable(KuduClient kuduClient, SchemaTableName tableName)
//    {
//        return tables.get(tableName);
//    }

    public Map<SchemaTableName, KuduTableHandle> getTables(KuduClient kuduClient)
    {
        Map<SchemaTableName, KuduTableHandle> tables = null;
        ImmutableMap.Builder<SchemaTableName, KuduTableHandle> tablesBuilder = ImmutableMap.builder();
//        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();
        List<String> listTable = null;
        try {
            listTable = kuduClient.getTablesList().getTablesList();
        }
        catch (KuduException e) {
            e.printStackTrace();
        }

        for (String table : listTable) {
            SchemaTableName schemaTableName = new SchemaTableName(PRESTO_KUDU_SCHEMA, table);
            tablesBuilder.put(schemaTableName, new KuduTableHandle(schemaTableName));
        }

        tables = tablesBuilder.build();

        return tables;
    }

    public List<ColumnMetadata> getColumns(KuduClient kuduClient, KuduTableHandle tableHandle)
    {
        List<ColumnMetadata> columnMetadatas = new ArrayList<ColumnMetadata>();
        String tableName = tableHandle.getSchemaTableName().getTableName();
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            List<ColumnSchema> columnSchemas = kuduTable.getSchema().getColumns();
            for (ColumnSchema columnSchema : columnSchemas) {
                ColumnMetadata columnMetadata = null;

                switch (columnSchema.getType()) {
                    case STRING:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), createUnboundedVarcharType());
                        break;
                    case BOOL:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), BOOLEAN);
                        break;
                    case INT8:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), TINYINT);
                        break;
                    case INT16:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), SMALLINT);
                        break;
                    case INT32:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), INTEGER);
                        break;
                    case INT64:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), BIGINT);
                        break;
                    case FLOAT:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), DOUBLE);
                        break;
                    case DOUBLE:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), DOUBLE);
                        break;
                    case UNIXTIME_MICROS:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), BIGINT);
                        break;
                }
                if (null != columnMetadata) {
                    columnMetadatas.add(columnMetadata);
                }
                else {
                    throw new IllegalArgumentException("The provided data type doesn't map" +
                            " to know any known one.");
                }
            }
        }
        catch (KuduException e) {
            logger.error(e, "%s open failed", tableName);
        }
        return columnMetadatas;
    }
}
