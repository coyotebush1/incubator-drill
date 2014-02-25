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

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;

import org.apache.drill.exec.ops.FragmentContext;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.type.SqlTypeName;

/**
 * OptiqProvider provides information schema rows for the various tables.
 * Each table has its own nested class, keeping them grouped together.
 */
public class OptiqProvider  {


  static public class Tables extends Abstract { 
    Tables(FragmentContext context) {
      super(context);
    }

    @Override
    public boolean visitTable(String schema, String tableName, Table table) {
      return writeRow("DRILL", schema, tableName, table.getJdbcTableType().toString());
    }
  }



  static public class Schemata extends Abstract {
    @Override
    public boolean visitSchema(String schemaName, Schema schema) {
      writeRow("DRILL", schemaName, "OWNER");
      return false;
    }

    Schemata(FragmentContext context) {
      super(context);
    }
  }

  
  
  static public class Columns extends Abstract {

    public Columns(FragmentContext context) {
      super(context);
    }

    @Override
    public boolean visitField(String schemaName, String tableName, RelDataTypeField field) {
      String columnName = field.getName();
      SqlTypeName type = field.getType().getSqlTypeName();

      writeRow("DRILL", schemaName, tableName, columnName, field.getIndex(),
          type.getName());  // TODO: more fields.
      return false;
    }
  }



  public static class Views extends Abstract {
    public Views(FragmentContext context) {
      super(context);
    }
    @Override
    public boolean visitTable(String schemaName, String tableName, Table table) {
      if (table.getJdbcTableType() == Schema.TableType.VIEW) {
        writeRow("DRILL", schemaName, tableName, "TODO: GetViewDefinition");
      }
      return false;
    }
  }




  public static class Abstract extends OptiqScanner {
    FragmentContext context;
    
    protected Abstract(FragmentContext context) {
      this.context = context;
    }

    
    @Override
    public void generateRows() {
      
      // Get the root schema from the context
      Schema root = null;  // TODO:
      
      // Scan the root schema for subschema, tables, columns.
      scanSchema(root); 
    }
  }


  /**
   * An OptiqScanner scans the Optiq schema, generating rows for each 
   * schema, table or column. It is intended to be subclassed, where the
   * subclass does what it needs when visiting a Optiq schema structure.
   */
  abstract static class OptiqScanner extends PipeProvider {
  

    /**
     *  Each visitor implements the following interface.
     *  If the schema visitor returns true, then visit the tables.
     *  If the table visitor returns true, then visit the fields (columns).
     */
    public boolean visitSchema(String schemaName, Schema schema){return true;}
    public boolean visitTable(String schemaName, String tableName, Table table){return true;}
    public boolean visitField(String schemaName, String tableName, RelDataTypeField field){return true;}



    protected void scanSchema(Schema root) {
      scanSchema(root.getName(), root);
    }
    /**
     * Recursively scan the schema, invoking the visitor as appropriate.
     * @param schemaPath - the path to the current schema, so far,
     * @param schema - the current schema.
     * @param visitor - the methods to invoke at each entity in the schema.
     */
    private void scanSchema(String schemaPath, Schema schema) {

      // Recursively scan the subschema.
      for (String name: schema.getSubSchemaNames()) {
        scanSchema(schemaPath + "." + name, schema.getSubSchema(name));
      }

      // Visit this schema and if requested ...
      if (visitSchema(schemaPath, schema)) {

        // ... do for each of the schema's tables.
        for (String tableName: schema.getTableNames()) {
          Table table = schema.getTable(tableName);

          // Visit the table, and if requested ...
          if (visitTable(schemaPath,  tableName, table)) {

            // ... do for each of the table's fields.
            RelDataType tableRow = table.getRowType(null); // TODO: fill this in.
            for (RelDataTypeField field: tableRow.getFieldList()) {

              // Visit the field.
              visitField(schemaPath,  tableName, field);
            }
          }
        }
      }
    }
  }
}

