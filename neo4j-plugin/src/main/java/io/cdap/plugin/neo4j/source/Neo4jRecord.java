/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.neo4j.source;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * Writable class for Neo4j Source/Sink.
 */
public class Neo4jRecord extends DBRecord {
  private static final Logger LOG = LoggerFactory.getLogger(Neo4jRecord.class);

  public Neo4jRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    super(record, columnTypes);
  }

  public Neo4jRecord() {
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new Neo4jSchemaReader();
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (Types.JAVA_OBJECT == sqlType) {
      handleSpecificType(resultSet, recordBuilder, field, columnIndex);
    } else {
      setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  private void handleSpecificType(ResultSet resultSet,
                                  StructuredRecord.Builder recordBuilder,
                                  Schema.Field field, int columnIndex) throws SQLException {
    if (resultSet.getObject(columnIndex) instanceof byte[]) {
      recordBuilder.set(field.getName(), resultSet.getObject(columnIndex));
    } else {
      recordBuilder.set(field.getName(), resultSet.getObject(columnIndex, Map.class));
    }
  }
}
