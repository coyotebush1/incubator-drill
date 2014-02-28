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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.TableFunction;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;



/**
 * Wishful interfaces to build up and create an Optiq Schema. 
 * (might be nice if Optiq defined them this way)
 * The primary contribution of this class is to add a "setParent()"
 * method to the interface. Doesn't sound like much, but it enables
 * a properly formed subschema tree to be added to a properly formed schema,
 * no matter what the underlying implementation. Additionally, these interfaces separate
 * the methods which modify a schema from the methods which query a schema.
 */

/**
 * An interface for scanning an Optiq schema structure.
 * None of these methods modify the schema.
 */
interface Schema {

  /**
   * Returns the name of this schema.
   * <p>
   * The name must not be null, and must be unique within its parent.
   * The root schema is typically named "".
   */
  String getName();

  
  /** Returns the parent schema, or null if this schema has no parent. */
  Schema getParentSchema();

  /** Once a schema is immutable, it can be cached and need not be scanned ever again. */
  boolean isMutable();
  
  
  
  /** Returns the names of this schema's subschemas. */
  Set<String> getSubSchemaNames();

  /** Returns the sub-schema with the given name, or null.*/
  Schema getSubSchema(String name);


  /** Returns the table with the given name, or null if not found. */
  Table getTable(String name);

  /** Returns the names of the tables in this schema. */
  Set<String> getTableNames();



  /** Returns the names of the table functions in this schema. */
  Set<String> getTableFunctionNames();
  
  /** Returns a list of table functions in this schema with the given name */
  Collection<TableFunction> getTableFunctions(String name); 
}


/**
 * An extension of the Schema interface to allow construction of Schema trees.
 * These are the interfaces which modify Schema.
 * <p>
 * In general, schema objects will implement the MutableSchema interface.
 * Methods which examine (but don't modify) the schema will use the Schema interface.
 */
interface MutableSchema extends Schema {

  /** Adds a schema as a subschema to this subschema */
  void add(MutableSchema schema);

  /** Adds a table to this schema. */
  void add(String name, Table table);

  /** Adds a table function to this schema. */
  void add(String name, TableFunction table);
  
  /** 
   * Sets the parent relationship. 
   * This is a "private" interface to be used internally by SchemaPlus implementations.
   * In particular, it allows one implementation of SchemaPlus to add another
   * implementation of SchemaPlus as a child without having to know the details
   * of the new child. Note that parentSchema can be null.
   */
  void setParentSchema(MutableSchema parentSchema);
  
  /**
   * Sets the name of a root schema. This allows a schema name to be filled in after it is created.
   * Once a schema has a parent, its name cannot be changed.
   */
  void setName(String name);
  
  /**
   * Once a schema becomes immutable, the schema (and it's subschema?) may not be modified.
   * (Data from immutable schema can be cached. Data from mutable schema must be rescanned.)
   */
  void setMutable(boolean mutable);
  
  
  /**
   * Remove a subschema from current schema.
   */
  MutableSchema removeSubSchema(String Name);
}


/**
 * Wishful general class for building up a schema tree.
 * It builds and maintains and modifies the structure of a schema. 
 * Subclasses can add payload fields and methods as needed.
 */
public class DrillSchema implements MutableSchema {

  String name = "";
  MutableSchema parent = null;
  boolean mutable = false;
  Map<String, MutableSchema> subSchema = new HashMap<String, MutableSchema>();
  Map<String, Table>  tables = new HashMap<String, Table>();
  Multimap<String, TableFunction> tableFunctions = HashMultimap.create();
  

  public DrillSchema() {
  }
  
  public DrillSchema(String name) {
    super();
    this.name = name;
  }
  
  public DrillSchema(String name, MutableSchema parent) {
    this(name);
    this.parent = parent;
    parent.add(this);
  }
  
  
  ///////////////////////////////////////////////////////////
  // Methods to implement the NewSchema interface  (Schema?)
  ///////////////////////////////////////////////////////////
  
  @Override
  public String getName() {
    return name;
  }
  
  @Override
  public Schema getParentSchema() {
    return parent;
  }

  @Override
  public boolean isMutable() {
    return mutable;
  }

  
  
  @Override
  public Set<String> getTableNames() {
    return tables.keySet();
  }

  @Override
  public Table getTable(String name) {
    return tables.get(name);
  }



  @Override
  public Set<String> getTableFunctionNames() {
    return tableFunctions.keySet();
  }

  @Override
  public Collection<TableFunction> getTableFunctions(String name) {
    return tableFunctions.get(name);
  }


  @Override
  public Set<String> getSubSchemaNames() {
    return subSchema.keySet();
  }
  
  @Override
  public Schema getSubSchema(String name) {
    return (Schema)subSchema.get(name);
  }



  ////////////////////////////////////////////////////////////////////
  // Methods to implement NewSchemaPlus  (MutableSchema?)
  ///////////////////////////////////////////////////////////////////

  @Override
  public void setParentSchema(MutableSchema parent) {
    this.parent = parent;
  }
  

  @Override
  public void setName(String name) {
    if (parent != null) return; // TODO: throw exception?
    this.name = name;
  }
  
  @Override
  public void setMutable(boolean mutable) {
    this.mutable = mutable;
  }
  

  @Override
  public void add(MutableSchema schema) {
    if (schema.getParentSchema() != null) return;  // TODO: throw exception?
    
    subSchema.put(schema.getName(), schema);
    schema.setParentSchema(this);
  }
  
  @Override
  public MutableSchema removeSubSchema(String name) {
    MutableSchema sub = subSchema.remove(name);
    if (sub != null) {
      sub.setParentSchema(null);
    }
    return sub;
  }
  

  @Override
  public void add(String name, Table table) {
    tables.put(name, table);
  }

  @Override
  public void add(String name, TableFunction table) {
    tableFunctions.put(name, table);
  }




  /** 
   * Utility function to transfer all subschema to another schema.
   * Useful when creating a nameless "root" which needs to be merged into
   * a bigger schema. This static method works across all implementations of MutableSchema
   */
   public static void transferAllSubSchema(MutableSchema oldParent, MutableSchema newParent) {
     
     // Do for each subschema
     for (String name: oldParent.getSubSchemaNames()) {
       
       // Move it from the old parent to the new.
       MutableSchema schema = oldParent.removeSubSchema(name);
       newParent.add(schema);
     }
   }




}
