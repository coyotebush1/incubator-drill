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
package org.apache.drill.exec.store.easy.json;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin.JSONFormatConfig;

import com.fasterxml.jackson.annotation.JsonTypeName;

public class JSONFormatPlugin extends EasyFormatPlugin<JSONFormatConfig> {

  public JSONFormatPlugin(DrillbitContext context, DrillFileSystem fs, DrillFileSystem config) {
    this(context, fs, config, new JSONFormatConfig());
  }
  
  public JSONFormatPlugin(DrillbitContext context, DrillFileSystem fs, DrillFileSystem config, JSONFormatConfig formatPluginConfig) {
    super(context, fs, config, formatPluginConfig, true, false, false, "json", "json");
  }
  
  @Override
  public RecordReader getRecordReader(FragmentContext context, FileWork fileWork, FieldReference ref,
      List<SchemaPath> columns) throws ExecutionSetupException {
    return new JSONRecordReader(context, fileWork.getPath(), this.getFileSystem().getUnderlying(), ref, columns);
  }

  @JsonTypeName("json")
  public static class JSONFormatConfig implements FormatPluginConfig {
  }
}
