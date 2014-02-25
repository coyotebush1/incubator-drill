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

import java.util.ArrayList;
import java.util.ListIterator;

/**
 * PipeProvider sets up the framework so some subclass can "write" rows
 * to a an internal pipe, which another class (RowRecordReader) can "read" to
 * build up a record batch.
 * <p>
 * (Note that in our case, a "pipe" is really just a wrapper around a list.)
 */
public abstract class PipeProvider implements RowProvider {
  ArrayList<Object[]> pipe = null;
  ListIterator<Object[]> iter;
  
  abstract void generateRows();
  
  public boolean hasNext() {
    if (pipe == null) {
      pipe = new ArrayList<Object[]>();
      generateRows();
      iter = pipe.listIterator();
    }
    
    return iter.hasNext();
  }
  
  public Object[] next() {
    return iter.next();
  }
  
  public void previous() {
    iter.previous();
  }
  
  protected boolean writeRow(Object...values) {
    pipe.add(values);
    return true;
  }
  
}
  