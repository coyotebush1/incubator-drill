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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.drill.exec.store.AbstractSchema;
import org.junit.Assert;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.TableFunction;



/**
 * Class to build up and create an Optiq Schema.
 * The primary contribution of this class is to add a "setParent()"
 * method to the interface. Doesn't sound like much, but it enables
 * a properly formed subschema to be added to a properly formed schema,
 * long after both have been independently constructed.
 * Probably should be part of SchemaPlus interface if different
 * implementations need to be combined together.
 */

interface NewSchema {
  
}

interface NewSchemaPlus extends NewSchema {
  
}

public class FixedSchema implements NewSchemaPlus, SchemaPlus {

  String name;
  SchemaPlus parent = null;
  Map<String, SchemaPlus> subSchema = new HashMap<String, SchemaPlus>();
  Map<String, Table>  tables = new HashMap<String, Table>();
  

  public FixedSchema(String name) {
    super();
    this.name = name;
  }
  
  public FixedSchema(String name, SchemaPlus parent) {
    this(name);
    this.parent = parent;
    parent.add(this);
  }
  
  @Override
  public String getName() {
    return name;
  }
  

  @Override
  public SchemaPlus getParentSchema() {
    return parent;
  }
  
  
  public void setParentSchema(SchemaPlus parent) {
    this.parent = parent;
  }

  @Override
  public Table getTable(String name) {
    return tables.get(name);
  }


  @Override
  public Set<String> getTableNames() {
    return tables.keySet();
  }


  @Override
  public Collection<TableFunction> getTableFunctions(String name) {
    return Collections.emptySet();
  }

  @Override
  public Set<String> getTableFunctionNames() {
    return Collections.emptySet();
  }


  @Override
  public SchemaPlus getSubSchema(String name) {
    return subSchema.get(name);
  }


  @Override
  public Set<String> getSubSchemaNames() {
    return subSchema.keySet();
  }



  @Override
  public boolean isMutable() {
    return false;  // Will Optiq rescan us if "true"?
  }

  // add() is a mutator, so should probably be moved to SchemaPlus.
  //  Additionally, it seems funny that we add as a schema, but fetch as schemaplus.
  @Override
  public SchemaPlus add(Schema schema) {
    assert schema.getParentSchema() == this;
    subSchema.put(schema.getName(),  (SchemaPlus)schema);
    return this;   // TODO: verify return value.
  }


  @Override
  public void add(String name, Table table) {
    tables.put(name, table);
  }

  

  ///////////////////////////////////////////////
  // Not used for now. Not sure how Optiq uses these.
  ///////////////////////////////////////////////
  @Override
  public <T> T unwrap(Class<T> clazz) {
    return null;
  }
  @Override
  public SchemaPlus addRecursive(Schema schema) {
    return null;
  }
  @Override
  public void add(String name, TableFunction table) {
  }
  @Override
  public Expression getExpression() {
    return null;
  }
}
