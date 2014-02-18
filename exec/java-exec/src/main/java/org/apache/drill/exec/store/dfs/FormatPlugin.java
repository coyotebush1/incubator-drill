package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.QueryOptimizerRule;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;

/**
 * Similar to a storage engine but built specifically to work within a FileSystem context.
 */
public interface FormatPlugin {

  public boolean supportsRead();

  public boolean supportsWrite();
  
  public FormatMatcher getMatcher();
  
  public AbstractGroupScan getGroupScan(FieldReference outputRef, FileSelection selection) throws IOException;

  public List<QueryOptimizerRule> getOptimizerRules();
  
  public FormatPluginConfig getConfig();
  public StoragePluginConfig getStorageConfig();
  public DrillFileSystem getFileSystem();
  public DrillbitContext getContext();
  
}
