package org.apache.calcite.plan.pvd;

import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.rules.PvdRules;
import org.apache.calcite.rel.RelNode;

import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

import org.checkerframework.checker.nullness.qual.Nullable;

public class PvdPlanner extends AbstractRelOptPlanner{

  private RelNode root;
  private PvdRuleDriver driver;
  private RelNode bestPlan;
  private Set<PvdPattern> config;
  private LinkedList<RelOptRule> rules;
  private PvdRuleDriverType driverType;
  private Map<Integer, List<Integer>> bannedMerge;
  private Set<Integer> bannedPartition;
  private int plansFound;

  public PvdPlanner(){
    this(PvdRuleDriverType.SINGLE_RULE);
  }

  public PvdPlanner(PvdRuleDriverType driverType){
    this(null, driverType);
  }

  public PvdPlanner(RelNode root, PvdRuleDriverType driverType){
    super(RelOptCostImpl.FACTORY, null);
    this.root = root;
    driver = makeDriver(driverType);
    this.driverType = driverType;
    rules = new LinkedList<RelOptRule>();
    bestPlan = null;
    config = null;
    bannedMerge = new HashMap<Integer, List<Integer>>();
    bannedPartition = new HashSet<Integer>();
    plansFound = 1;
  }

  public void clearHistory(){
    bannedMerge.clear();
  }

  public void banPartition(int nodeID){
    bannedPartition.add(nodeID);
  }

  public boolean isBannedPartition(int nodeID){
    return bannedPartition.contains(nodeID);
  }

  public void banMerge(int pid, int cid){
    if (bannedMerge.containsKey(pid))
      bannedMerge.get(pid).add(cid);
    else{
      List<Integer> banList = new LinkedList<Integer>();
      banList.add(cid);
      bannedMerge.put(pid, banList);
    }
  }

  public boolean isBannedMerge(int pid, int cid){
    if (!bannedMerge.containsKey(pid))
      return false;
    else
      return bannedMerge.get(pid).contains(cid);
  }

  public void setPlansFound(int n){
    this.plansFound = n;
  }

  public int numPlansFound(){
    return plansFound;
  }

  public void addAllPvdRules(){
    addRule(PvdRules.HASH_INDEX_SCAN);
    addRule(PvdRules.GENERIC_CUBE_AGG);
    addRule(PvdRules.HASHCUBE_AGG);
    addRule(PvdRules.BITFILTER);
    addRule(PvdRules.PARTITION_FILTER_EXPR);
    addRule(PvdRules.PUSH_ANY_FILTER);
    addRule(PvdRules.PUSH_FILTER_EXPR);
    addRule(PvdRules.PARTITION_PROJECT_EXPR);
    addRule(PvdRules.PUSH_ANY);
    addRule(PvdRules.PARTITION_ANY);
    addRule(PvdRules.PARTITION_AGGS_EXPR);
  }

  public PvdRuleDriver makeDriver(PvdRuleDriverType driverType){
    switch(driverType){
    case DFS:
      return new DfsDriver(this);
    case SINGLE_RULE:
    default:
      return new SingleRuleDriver(this);
    }
  }

  @Override public RelNode findBestExp(){
    if (root == null){
      throw new RuntimeException("Root has not been set for planner");
    }
    driver.drive();
    return root;
  }

  public void setConfig(Set<PvdPattern> config){
    this.config = config;
  }

  @Override public boolean addRule(RelOptRule rule) {
    if(driverType == PvdRuleDriverType.SINGLE_RULE){
      rules.clear();
    }
    rules.add(rule);
    return true;
  }

  @Override public RelNode register(
      RelNode rel,
      @Nullable RelNode equivRel) {
    return rel;
  }

  // ignore
  @Override public RelNode changeTraits(RelNode rel, RelTraitSet toTraits) {
    return rel;
  }

  @Override public void setRoot(RelNode rel) {
    root = rel;
  }

  @Override public List<RelOptRule> getRules() {
    return rules;
  }

  // implement RelOptPlanner
  @Override public RelNode ensureRegistered(RelNode rel, @Nullable RelNode equivRel) {
    return rel;
  }

  // implement RelOptPlanner
  @Override public boolean isRegistered(RelNode rel) {
    return true;
  }

  @Override public @Nullable RelNode getRoot() {
    return root;
  }
}
