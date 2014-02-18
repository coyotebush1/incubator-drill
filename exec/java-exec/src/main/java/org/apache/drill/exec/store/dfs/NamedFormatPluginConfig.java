package org.apache.drill.exec.store.dfs;

import com.fasterxml.jackson.annotation.JsonTypeName;


@JsonTypeName("named")
public class NamedFormatPluginConfig implements FormatPluginConfig{
  public String name;
}
