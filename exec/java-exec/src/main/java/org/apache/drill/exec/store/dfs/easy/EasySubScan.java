package org.apache.drill.exec.store.dfs.easy;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FormatPluginConfig;
import org.apache.drill.exec.store.schedule.CompleteFileWork.FileWorkImpl;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EasySubScan extends AbstractSubScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasySubScan.class);

  private final List<FileWorkImpl> files;
  private final EasyFormatPlugin<?> formatPlugin;
  private final FieldReference ref;
  private final List<SchemaPath> columns;
  
  @JsonCreator
  public EasySubScan(
      @JsonProperty("files") List<FileWorkImpl> files, //
      @JsonProperty("storage") StoragePluginConfig storageConfig, //
      @JsonProperty("format") FormatPluginConfig formatConfig, //
      @JacksonInject StoragePluginRegistry engineRegistry, // 
      @JsonProperty("ref") FieldReference ref, //
      @JsonProperty("columns") List<SchemaPath> columns //
      ) throws IOException, ExecutionSetupException {
    
    this.formatPlugin = (EasyFormatPlugin<?>) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    this.files = files;
    this.ref = ref;
    this.columns = columns;
  }
  
  public EasySubScan(List<FileWorkImpl> files, EasyFormatPlugin<?> plugin, FieldReference ref, List<SchemaPath> columns){
    this.formatPlugin = plugin;
    this.files = files;
    this.ref = ref;
    this.columns = columns;
  }
  
  public EasyFormatPlugin<?> getFormatPlugin(){
    return formatPlugin;
  }

  @JsonProperty("files")
  public List<FileWorkImpl> getWorkUnits() {
    return files;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig(){
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig(){
    return formatPlugin.getConfig();
  }
  
  @JsonProperty("ref")
  public FieldReference getRef() {
    return ref;
  }
  
  @JsonProperty("columns")
  public List<SchemaPath> getColumns(){
    return columns;
  }

   
}
