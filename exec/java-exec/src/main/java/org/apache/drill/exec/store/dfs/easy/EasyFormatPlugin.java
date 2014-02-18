package org.apache.drill.exec.store.dfs.easy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.QueryOptimizerRule;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatPluginConfig;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;

import com.beust.jcommander.internal.Lists;

public abstract class EasyFormatPlugin<T extends FormatPluginConfig> implements FormatPlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyFormatPlugin.class);

  private final BasicFormatMatcher matcher;
  private final DrillbitContext context;
  private final boolean readable;
  private final boolean writable;
  private final boolean blockSplittable;
  private final DrillFileSystem fs;
  
  protected EasyFormatPlugin(DrillbitContext context, DrillFileSystem fs, DrillFileSystem config, T formatPluginConfig, boolean readable, boolean writable, boolean blockSplittable, String extension){
    this.matcher = new BasicFormatMatcher(this, fs, extension);
    this.readable = readable;
    this.writable = writable;
    this.context = context;
    this.blockSplittable = blockSplittable;
    this.fs = fs;
  }
  
  @Override
  public DrillFileSystem getFileSystem() {
    return fs;
  }

  @Override
  public DrillbitContext getContext() {
    return context;
  }

  /**
   * Whether or not you can split the format based on blocks within file boundaries. If not, the simple format engine will
   * only split on file boundaries.
   * 
   * @return True if splittable.
   */
  public boolean isBlockSplittable(){
    return blockSplittable;
  };

  public abstract RecordReader getRecordReader(FragmentContext context, FileWork fileWork, FieldReference ref, List<SchemaPath> columns) throws ExecutionSetupException;

  
  RecordBatch getBatch(FragmentContext context, EasySubScan scan) throws ExecutionSetupException {
    List<RecordReader> readers = Lists.newArrayList();
    for(FileWork work : scan.getWorkUnits()){
      readers.add(getRecordReader(context, work, scan.getRef(), scan.getColumns())); 
    }
    
    return new ScanBatch(context, readers.iterator());
  }
  
  @Override
  public AbstractGroupScan getGroupScan(FieldReference outputRef, FileSelection selection) throws IOException {
    return null;
  }

  @Override
  public FormatPluginConfig getConfig() {
    return null;
  }

  @Override
  public StoragePluginConfig getStorageConfig() {
    return null;
  }

  @Override
  public boolean supportsRead() {
    return readable;
  }

  @Override
  public boolean supportsWrite() {
    return writable;
  }

  @Override
  public FormatMatcher getMatcher() {
    return matcher;
  }

  @Override
  public List<QueryOptimizerRule> getOptimizerRules() {
    return Collections.emptyList();
  }

  
}
