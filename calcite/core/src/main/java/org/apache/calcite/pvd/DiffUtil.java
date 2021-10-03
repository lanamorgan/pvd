package org.apache.calcite.pvd;

import org.apache.calcite.plan.pvd.PvdRuleCall;
import org.apache.calcite.plan.pvd.PartitionRuleCall;
import org.apache.calcite.plan.pvd.MergeRuleCall;
import org.apache.calcite.plan.pvd.PushRuleCall;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.rel.logical.LogicalAny;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelAnyType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexAny;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class DiffUtil {

  public static List<PvdRuleCall> generateAllPush(RexNode root, PvdRuleCall call){
    List<PvdRuleCall> ruleCalls = new ArrayList<>();
    Queue<RexNode> q = new LinkedList<RexNode>();
    q.add(root);
    while(q.peek() != null) {
      RexNode curr = q.poll();
      if (curr instanceof RexCall && !(curr instanceof RexAny)){
        RexCall rexCall = (RexCall) curr;
        for(RexNode op: rexCall.getOperands()) {
          if (op instanceof RexAny){
            RexAny any = (RexAny) op;
            ruleCalls.add(PushRuleCall.create(call, any.getNodeID()));
          }
          else{
            q.add(op);
          }
        }
      }
    }
    return ruleCalls;
  }

  public static int getIndexOfID(int nodeID, List<RexNode> list){
    for(int i = 0; i < list.size(); i++){
      if (list.get(i) instanceof RexAny){
        RexAny any = (RexAny) list.get(i);
        if (any.getNodeID() == nodeID)
          return i;
      }
    }
    return -1;
  }

  public static List<PvdRuleCall> generateAllPushList(List<RexNode> list, PvdRuleCall call){
    List<PvdRuleCall> ruleCalls = new ArrayList<>();
    for(RexNode rx: list){
      if (rx instanceof RexAny){
        RexAny any = (RexAny) rx;
        ruleCalls.add(PushRuleCall.create(call, any.getNodeID()));
      }
    }
    return ruleCalls;
  }

  public static RexNode pushAnyExpr(RexNode root, int nodeID) {
    if (root instanceof RexCall) {
      RexCall rc = (RexCall) root;
      int pushIndex = getPushIndex(rc, nodeID);
      if (pushIndex >= 0) {
        RexAny any = (RexAny) rc.getOperands().get(pushIndex);
        List<RelDataType> anyType = new LinkedList<>();
        List<RexNode> pushedChildren = new LinkedList<>();
        for (RexNode child:  any.getOperands()){
          List<RexNode> clonedOps = new ArrayList<RexNode>();
          for (RexNode op : rc.getOperands()) {
            clonedOps.add(pushAnyExpr(op, nodeID));
          }
          clonedOps.set(pushIndex, child);
          pushedChildren.add(rc.clone(rc.getType(), clonedOps));
          anyType.add(rc.getType());
        }
        return new RexAny(makeAnyType(anyType), pushedChildren);
      }
      else {
        List<RexNode> ops = rc.getOperands();
        List<RexNode> clonedOps = new ArrayList<RexNode>();
        for (RexNode op : ops) {
          clonedOps.add(pushAnyExpr(op, nodeID));
        }
        //fix type
        return rc.clone(rc.getType(), clonedOps);
      }
    }
    return root;
  }

  private static int getPushIndex(RexCall call, int nodeID){
    List<RexNode> ops = call.getOperands();
    for(int i = 0; i < ops.size(); i++){
      if (ops.get(i) instanceof RexAny){
        RexAny any = (RexAny) ops.get(i);
        if(any.getNodeID() == nodeID)
          return i;
      }
    }
    return -1;
  }

  public static List<PvdRuleCall> generatePartitions(LogicalAny root, PvdRuleCall call) {
    List<RelNode> inputs = root.getInputs();
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    List<PvdRuleCall> ruleCalls = new ArrayList<>();
    if (root.getInputs().size() <= 1 || planner.isBannedPartition(root.getNodeID()) ||
        root.getInputs().stream().allMatch(n -> n instanceof LogicalAny))
      return ruleCalls;
    planner.banPartition(root.getNodeID());
    List<Integer> ops = makeSequence(inputs.size());
    //minimum partition size is 2
    for (int i = 2; i <= ops.size(); i++) {
      List<List<List<Integer>>> ret = partitionN(ops, i);
      for (List<List<Integer>> l : ret) {
        int[][] pmap = new int[l.size()][];
        for (int j = 0; j < l.size(); j++) {
          pmap[j] = toIntArray(l.get(j));
        }
        ruleCalls.add(PartitionRuleCall.create(call, root.getNodeID(), pmap));
      }
    }
    return ruleCalls;
  }

  public static RelNode copyRelNode(RelNode root){
    List<RelNode> inputs = root.getInputs();
    List<RelNode> copyInput = new ArrayList<>();
    for(RelNode input : inputs){
      copyInput.add(copyRelNode(input));
    }
    return root.copy(root.getTraitSet(), copyInput);
  }

  public static List<PvdRuleCall> generatePartitions(RexNode root, PvdRuleCall call){
    List<PvdRuleCall> ruleCalls = new ArrayList<>();
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    Queue<RexNode> q = new LinkedList<RexNode>();
    q.add(root);
    while(q.peek() != null){
      RexNode curr = q.poll();
      if (curr instanceof RexAny) {
        RexAny any = (RexAny) curr;
        if (any.getOperands().size() > 1 && !planner.isBannedPartition(any.getNodeID()) &&
            !any.getOperands().stream().allMatch(n -> n instanceof RexAny)) {
          planner.banPartition(any.getNodeID());
          //generate partition maps
          List<Integer> ops = makeSequence(any.getOperands().size());
          //minimum partition size is 2
          for (int i = 2; i <= ops.size(); i++) {
            List<List<List<Integer>>> ret = partitionN(ops, i);
          /*
           There's no reason on earth to spend this much effort converting List<List<Integer>>
           to int[][] other than int [][] is what I started with and it's easier to do this
           monstrosity than it is to change the code everywhere else.
           */
            for (List<List<Integer>> l : ret) {
              int[][] pmap = new int[l.size()][];
              for (int j = 0; j < l.size(); j++) {
                pmap[j] = toIntArray(l.get(j));
              }
              ruleCalls.add(PartitionRuleCall.create(call, any.getNodeID(), pmap));
            } // finish converting
          } // finish generating partitions
        }
      }
      if (curr instanceof RexCall){
        RexCall rexCall = (RexCall) curr;
        for(RexNode op: rexCall.getOperands())
          q.add(op);
      }
    }
    return ruleCalls;
  }

  private static int[] toIntArray(List<Integer> list){
    int[] ret = new int[list.size()];
    for(int i = 0;i < ret.length;i++) {
      ret[i] = list.get(i);
    }
    return ret;
  }

  private static List<Integer> makeSequence(int n) {
    List<Integer> ret = new ArrayList<>(n);
    for (int i=0; i<n; i++) {
      ret.add(i);
    }
    return ret;
  }

  // gratefully borrowed from SO
  private static List<List<List<Integer>>> partitionN(List<Integer> ori, int m) {
    List<List<List<Integer>>> ret = new ArrayList<>();
    if(ori.size() < m || m < 1) return ret;

    if(m == 1) {
      List<List<Integer>> partition = new ArrayList<>();
      partition.add(new ArrayList<>(ori));
      ret.add(partition);
      return ret;
    }

    // f(n-1, m)
    List<List<List<Integer>>> prev1 = partitionN(ori.subList(0, ori.size() - 1), m);
    for(int i=0; i<prev1.size(); i++) {
      for(int j=0; j<prev1.get(i).size(); j++) {
        List<List<Integer>> l = new ArrayList<>();
        for(List<Integer> inner : prev1.get(i)) {
          l.add(new ArrayList<>(inner));
        }
        l.get(j).add(ori.get(ori.size()-1));
        ret.add(l);
      }
    }
    List<Integer> set = new ArrayList<>();
    set.add(ori.get(ori.size() - 1));
    // f(n-1, m-1)
    List<List<List<Integer>>> prev2 = partitionN(ori.subList(0, ori.size() - 1), m - 1);
    for(int i=0; i<prev2.size(); i++) {
      List<List<Integer>> l = new ArrayList<>(prev2.get(i));
      l.add(set);
      ret.add(l);
    }
    return ret;
  }


  // if the node ID to be transformed is unknown (nodeID==0), find first match
  public static boolean canPartition(RexNode root, int nodeID){
    if (root instanceof RexAny) {
      RexAny any = (RexAny) root;
      if (nodeID == 0 || any.getNodeID() == nodeID) {
        return any.getOperands().size() > 1 &&
            !any.getOperands().stream().allMatch(n -> n instanceof RexAny);
      }
    }
    if (root instanceof RexCall){
      RexCall call = (RexCall) root;
      List<RexNode> ops = call.getOperands();
      boolean p = false;
      for(RexNode op : ops){
        p = p || (canPartition(op, nodeID));
      }
      return p;
    }
    else
      return false;
  }

  public static RexNode partition(RexNode root, int nodeID,
      int [][] partitionMap, PvdPlanner planner){
    if (root instanceof RexAny){
      RexAny any = (RexAny) root;
      if (nodeID == 0 || any.getNodeID() == nodeID) {
        List<RexNode> parent = any.getOperands();
        int numPartitions = partitionMap.length;
        List<RexNode> partitions = new ArrayList<RexNode>(numPartitions);
        List<RelDataType> partitionType = new ArrayList<RelDataType>();
        for(int i = 0; i < numPartitions; i++){
          List<RexNode> partitionChildren = new ArrayList<RexNode>();
          List<RelDataType> pType = new ArrayList<RelDataType>();
          int[] indexes = partitionMap[i];
          for(int index: indexes){
            partitionChildren.add(parent.get(index));
            pType.add(parent.get(index).getType());
          }
          RelDataType t = makeAnyType(pType);
          RexAny newAny = new RexAny(t, partitionChildren);
          partitions.add(newAny);
          planner.banPartition(newAny.getNodeID());
          partitionType.add(t);
        }
        return new RexAny(makeAnyType(partitionType), partitions);
      }
    }
    if (root instanceof RexCall){
      RexCall call = (RexCall) root;
      List<RexNode> ops = call.getOperands();
      List<RexNode> clonedOps = new ArrayList<RexNode>();
      for (RexNode op: ops){
        clonedOps.add(partition(op, nodeID, partitionMap, planner));
      }
      //fix type
      return call.clone(call.getType(), clonedOps);
    }
    else{
      return root;
    }
  }


  public static boolean canMerge(RexNode root, int pid, int cid){
    if (root instanceof RexAny) {
      RexAny any = (RexAny) root;
      if (pid == 0 || any.getNodeID() == pid) {
        return any.getOperands().get(cid) instanceof RexAny;
      }
    }
    if (root instanceof RexCall){
      RexCall call = (RexCall) root;
      List<RexNode> ops = call.getOperands();
      boolean p = false;
      for(RexNode op : ops){
        p = p || (canMerge(op, pid, cid));
      }
      return p;
    }
    else
      return false;
  }


  public static RexNode merge(RexNode root, int pid, int cid) {

    if (root instanceof RexAny){
      RexAny node = (RexAny) root;
      if (pid == 0 || node.getNodeID() == pid) {
        if (!(node.getOperands().get(cid) instanceof RexAny)){
          return node;
        }
        RelAnyType parent = (RelAnyType) node.getType();
        List <RelDataType> parentType = parent.childTypes;
        List <RelDataType> mergeTypes = parentType.subList(0, cid);
        ArrayList <RexNode> mutOp = new ArrayList<>(node.getOperands());
        List <RexNode> mergeChildren = new ArrayList<>(mutOp.subList(0, cid));
        // merge node
        RexAny mergeNode = (RexAny) node.getOperands().get(cid);
        RelAnyType mergeNodeType = (RelAnyType) mergeNode.getType();
        mergeTypes.addAll(mergeNodeType.childTypes);
        mergeChildren.addAll(mergeNode.getOperands());

        // add right side children
        mergeTypes.addAll(parentType.subList(cid+1, parentType.size()));
        mergeChildren.addAll(mutOp.subList(cid+1, mutOp.size()));
        return new RexAny(makeAnyType(mergeTypes), mergeChildren);
      }
    }
    if (root instanceof RexCall){
      RexCall call = (RexCall) root;
      List<RexNode> ops = call.getOperands();
      List<RexNode> clonedOps = new ArrayList<RexNode>();
      for (RexNode op: ops){
        clonedOps.add(merge(op, pid, cid));
      }
      //fix type
      return call.clone(call.getType(), clonedOps);
    }
    else{
      return root;
    }
  }


//
//  // push an ANY in an expression past a nonRexAny parent
//  public static RexAny push(RexCall parent, int child){
//    //parent cannot be an ANY node
//    if (parent instanceof RexAny)
//      return null;
//
//    if (!(parent.getOperands().get(child) instanceof RexAny))
//      return null;
//
//    RexAny push = (RexAny) parent.getOperands().get(child);
//    List <RexNode> oldChildren = push.getOperands();
//    List <RexNode> pChildren = new ArrayList<RexNode>();
//    List <RelDataType> outputType = new ArrayList<RelDataType>();
//    for (RexNode oChild: oldChildren){
//      List <RexNode> pushOperands = new ArrayList<>(parent.getOperands());
//      pushOperands.set(child, oChild);
//      pChildren.add(new RexCall(parent.getType(), parent.getOperator(), pushOperands));
//      outputType.add(parent.getType());
//    }
//    return new RexAny(makeAnyType(outputType), pChildren);
//  }
//
  private static RelDataType makeAnyType(List<RelDataType> types){
    List <RelDataTypeField> innerTypes = new ArrayList<RelDataTypeField>();
    for(int j = 0; j < types.size(); j++){
      innerTypes.add(new RelDataTypeFieldImpl("",j ,types.get(j)));
    }
    return new RelAnyType(innerTypes, types);
  }
}
