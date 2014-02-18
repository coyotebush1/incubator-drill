package org.apache.drill.exec.store;

import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("named")
public class NamedStoragePluginConfig implements StoragePluginConfig{
  public String name;
}
