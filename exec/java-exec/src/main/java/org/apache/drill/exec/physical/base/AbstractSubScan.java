package org.apache.drill.exec.physical.base;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.exec.physical.OperatorCost;

import com.google.common.collect.Iterators;

public abstract class AbstractSubScan implements SubScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSubScan.class);

  @Override
  public OperatorCost getCost() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Size getSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isExecutable() {
    return true;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return this;
  }

  @Override
  public void accept(GraphVisitor<PhysicalOperator> visitor) {
    visitor.enter(this);
    visitor.visit(this);
    visitor.leave(this);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }
  
}
