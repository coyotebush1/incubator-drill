package org.apache.drill.exec.store.dfs;

import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;

public class FileSystemFormatConfig<T extends FormatPluginConfig> implements StoragePluginConfig{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemFormatConfig.class);
  
  public T getFormatConfig(){
    return null;
  }
}
