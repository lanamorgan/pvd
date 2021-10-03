package org.apache.calcite.plan.pvd;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;


import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class Step{
  protected RelNode treeRoot;
  protected ArrayList<Integer> ptrPath;
  protected RelNode ptr;
  protected Set<PvdPattern> ds;
  protected List<PvdRuleCall> path;

  public Step(Set<PvdPattern> ds, RelNode tree, RelNode ptr){
    this.ds = ds;
    this.treeRoot = tree;
    this.ptr = ptr;
    this.ptrPath = new ArrayList<Integer>();
    this.path = new LinkedList<PvdRuleCall>();
  }

  public Step(Set<PvdPattern> ds, RelNode tree, RelNode ptr,
      ArrayList<Integer> ptrPath, List<PvdRuleCall> hist){
    this.ds = ds;
    this.treeRoot = tree;
    this.ptr = ptr;
    this.ptrPath= ptrPath;
    this.path = hist;
  }

  public long getCost(){
    return 0;
  }

  public long getSize() {
    return 0;
  }

  public Set<PvdPattern> getOpts(){
    return ds;
  }

  public void addOpt(PvdPattern p){
    ds.add(p);
  }

  public List<PvdRuleCall> getPath(){
    return path;
  }

  public RelNode getPtr(){
    return ptr;
  }



  public RelNode replacePtr(RelNode newRel){
    if(ptrPath.size() == 0){
      treeRoot = newRel;
      ptr = treeRoot;
      return treeRoot;
    }

    int lastIndex = ptrPath.size()-1;
    RelNode start = treeRoot;
    for(int i = 0; i < lastIndex; i++){
      start = start.getInput(ptrPath.get(i));
    }
//    System.out.println("=====DEBUG=====");
//    System.out.println("BEFORE:");
//    System.out.println(RelOptUtil.toString(treeRoot));
    start.replaceInput(ptrPath.get(lastIndex), newRel);
//    System.out.println("AFTER:");
//    System.out.println(RelOptUtil.toString(treeRoot));
    ptr = treeRoot;
    ptrPath.clear();
    return ptr;
  }

  public void setPtr(RelNode ptr){
    this.ptr = ptr;
  }

  public void addPtrPath(Integer i){
    this.ptrPath.add(i);
  }

  public RelNode getRoot(){
    return treeRoot;
  }

  // TODO: need to copy tree and data structures
  public Step copy(){
    RelNode newRoot = copyTree();
    RelNode copyPtr = newRoot;
    for(Integer i : ptrPath){
      copyPtr = copyPtr.getInputs().get(i);
    }
    return new Step(ds, newRoot, copyPtr,
        new ArrayList<Integer>(ptrPath),
        new LinkedList<PvdRuleCall>(path));
  }

  private RelNode copyTree(){
    return copyTree(treeRoot);
  }

  private RelNode copyTree(RelNode node){
    List<RelNode> inputs = new ArrayList<RelNode>();
    for (RelNode input : node.getInputs()){
      inputs.add(copyTree(input));
    }
    return node.copy(node.getTraitSet(), inputs);
  }

  public int hashCode() {
    return 0;
  }

  public boolean equals(Object obj){
    if (!(obj instanceof Step))
      return false;
    Step other = (Step) obj;
    List <PvdRuleCall> otherPath = other.getPath();
    if (otherPath.size() != path.size())
      return false;
    for (int i = 0; i < path.size(); i++){
      if (!path.get(i).equals(otherPath.get(i)))
        return false;
    }
    return true;
  }

  //TODO: for expr transformations, may need more than ruleCall for bookkeeping
  public void addStep(PvdRuleCall rc){
    path.add(rc);
  }
}
