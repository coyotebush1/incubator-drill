package org.apache.drill.exec.store.dfs;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property="type")
public class FormatSelection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatSelection.class);
  
  public FormatSelection(){}
  
  
  public FormatSelection(FormatPluginConfig format, FileSelection selection) {
    super();
    this.format = format;
    this.selection = selection;
  }


  public FormatPluginConfig format;
  public FileSelection selection;
  
}
