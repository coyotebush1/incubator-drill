package org.apache.drill.exec.store.dfs.easy;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

public class EasyBatchCreator implements BatchCreator<EasySubScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, EasySubScan config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return config.getFormatPlugin().getBatch(context, config);
  }
  
  
}
