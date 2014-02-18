package org.apache.drill.exec.store.dfs;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property="type")
public interface FormatPluginConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatPluginConfig.class);
}
