package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Arrays;

public class PvdRules {

  private PvdRules() {
  }

  public static final HashIndexScanRule HASH_INDEX_SCAN =
      HashIndexScanRule.Config.DEFAULT.toRule();

  public static final GenericCubeAggregateRule GENERIC_CUBE_AGG =
      GenericCubeAggregateRule.Config.DEFAULT.toRule();

  public static final HashCubeAggregateRule HASHCUBE_AGG =
      HashCubeAggregateRule.Config.DEFAULT.toRule();

  public static final PushAnyRule PUSH_ANY =
      PushAnyRule.Config.DEFAULT.toRule();

  public static final PushAnyFilterRule PUSH_ANY_FILTER =
      PushAnyFilterRule.Config.DEFAULT.toRule();

  public static final PushFilterExprRule PUSH_FILTER_EXPR =
      PushFilterExprRule.Config.DEFAULT.toRule();

  public static final PartitionProjectExprRule PARTITION_PROJECT_EXPR =
      PartitionProjectExprRule.Config.DEFAULT.toRule();

  public static final PartitionFilterExprRule PARTITION_FILTER_EXPR =
      PartitionFilterExprRule.Config.DEFAULT.toRule();

  public static final PartitionAnyRule PARTITION_ANY =
      PartitionAnyRule.Config.DEFAULT.toRule();

  public static final PartitionAggregatesExprRule PARTITION_AGGS_EXPR =
      PartitionAggregatesExprRule.Config.DEFAULT.toRule();

  public static final BitFilterRule BITFILTER = BitFilterRule.Config.DEFAULT.toRule();

  public static List<RelOptRule> PartitionRules = Arrays.asList(
      PARTITION_PROJECT_EXPR,
      PARTITION_AGGS_EXPR,
      PARTITION_ANY,
      PARTITION_FILTER_EXPR
  );

  public static List<RelOptRule> PushRules = Arrays.asList(
      PUSH_ANY,
      PUSH_ANY_FILTER,
      PUSH_FILTER_EXPR
  );

 public static List<RelOptRule> DsRules = Arrays.asList(
   GENERIC_CUBE_AGG,
   HASH_INDEX_SCAN,
   HASHCUBE_AGG,
   BITFILTER
 );

  public static boolean isPartitionRule(RelOptRule rule){
    return (rule instanceof PartitionAnyRule || rule instanceof PartitionProjectExprRule
        || rule instanceof PartitionFilterExprRule);
  }

  public static boolean isMergeRule(RelOptRule rule){
    return rule instanceof MergeFilterExprRule;
  }
}
