/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.infoschema;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.Var16CharVector;
import org.apache.drill.exec.vector.VarCharVector;

/**
 * InfoSchemaTable defines and provides the value vectors for Information Schema tables.
 * <p>
 * Each information schema table is defined as a nested class, keeping them all 
 * grouped together in a single file. The table classes do the following:
 * <p>Declare the table name.
 * <p>Declare the field names and types.
 * <p>Optionally define a method to write a row of data to the vectors.
 *    If no method is defined, a slower default method will kick in and do the job.
 */
public class InfoSchemaTable{

  public static class Schemata extends FixedTable {
    static final String tableName = "SCHEMATA";
    static final String[] fieldNames = {"CATALOG_NAME", "SCHEMA_NAME", "SCHEMA_OWNER"};
    static final MajorType[] fieldTypes = {VARCHAR,         VARCHAR,       VARCHAR};
      
    public Schemata() {
      super(tableName, fieldNames, fieldTypes);
    }
    
    public boolean writeRowToVectors(int index, Object[] row) {
      return 
        setSafe((VarCharVector)vectors.get(0), index, (String)row[0]) &&
        setSafe((VarCharVector)vectors.get(1), index,  (String)row[1]) &&
        setSafe((VarCharVector)vectors.get(2), index,  (String)row[2]);
    }
  }
  
  public static class Tables extends FixedTable {
    static final String tableName = "TABLES";
    static final String[] fieldNames = {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE"};
    static final MajorType[] fieldTypes = {VARCHAR,          VARCHAR,        VARCHAR,      VARCHAR};
    
    public boolean writeRowToVectors(int index, Object[] row) {
      return
        setSafe((VarCharVector)vectors.get(0), index, (String)row[0]) &&
        setSafe((VarCharVector)vectors.get(1), index, (String)row[1]) &&
        setSafe((VarCharVector)vectors.get(2), index, (String)row[2]) &&
        setSafe((VarCharVector)vectors.get(3), index, (String)row[3]);
    }
    
    public Tables() {
      super(tableName, fieldNames, fieldTypes);
      }  
  }
  
  public static class Columns extends FixedTable {
    static final String tableName = "COLUMNS";
    static final String[] fieldNames = {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME",
                                  "ORDINAL_POSITION", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH",
                                  "NUMERIC_PRECISION_RADIX", "NUMERIC_SCALE", "NUMERIC_PRECISION"};
    static final MajorType[] fieldTypes= { VARCHAR,         VARCHAR,       VARCHAR,      VARCHAR,
                                     INT,             VARCHAR,        VARCHAR,     INT,
                                     INT,             INT,            INT};
    public Columns() {
      super(tableName, fieldNames, fieldTypes);
    }
    

    @Override
    public boolean writeRowToVectors(int index, Object[] row) {
      return 
        setSafe((VarCharVector)vectors.get(0), index, (String)row[0]) &&
        setSafe((VarCharVector)vectors.get(1), index, (String)row[1]) &&
        setSafe((VarCharVector)vectors.get(2), index, (String)row[2]) &&
        setSafe((VarCharVector)vectors.get(3), index, (String)row[3]) && 
        setSafe((IntVector)vectors.get(4), index, (int)row[4]) &&
        setSafe((IntVector)vectors.get(5), index, (String)row[5]) &&
        setSafe((VarCharVector)vectors.get(6), index, (String)row[6]) &&
        setSafe((VarCharVector)vectors.get(7), index, (String)row[6]) &&
        setSafe((IntVector)vectors.get(8), index, (int)row[8]) &&
        setSafe((IntVector)vectors.get(9), index, (int)row[9]) &&
        setSafe((IntVector)vectors.get(10), index, (int)row[10]) &&
        setSafe((IntVector)vectors.get(11), index, (int)row[11]);
    }
  }

  
  static abstract public class Views extends FixedTable {
    static final String tableName = "VIEWS";
    static final String[] fieldNames = {"TABLE_CATALOG", "TABLE_SHEMA", "TABLE_NAME", "VIEW_DEFINITION"};
    static final MajorType[] fieldTypes = {VARCHAR,         VARCHAR,       VARCHAR,      VARCHAR};
 
   
    public boolean writeRowToVectors(int index, Object[] row) {
      return setSafe((VarCharVector)vectors.get(0), index, (String)row[0]) &&
          setSafe((VarCharVector)vectors.get(1), index, (String)row[1])  &&
          setSafe((VarCharVector)vectors.get(2), index, (String)row[2])    &&
          setSafe((VarCharVector)vectors.get(3), index, (String)row[3]);
    }
    
    Views() {
      super(tableName, fieldNames, fieldTypes);
    }
  }

}
