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

/**
 *
 */
public abstract class FixedTable extends EmptyVectorSet {
  String tableName;
  String[] fieldNames;
  MajorType[] fieldTypes;

  /* (non-Javadoc)
   * @see org.apache.drill.exec.store.ischema.VectorSet#createVectors(org.apache.drill.exec.memory.BufferAllocator)
   */
  @Override
  public void createVectors(BufferAllocator allocator) {
    createVectors(fieldNames, fieldTypes, allocator);
  }
  
  FixedTable(String tableName, String[] fieldNames, MajorType[] fieldTypes) {
    this.tableName = tableName;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

}
