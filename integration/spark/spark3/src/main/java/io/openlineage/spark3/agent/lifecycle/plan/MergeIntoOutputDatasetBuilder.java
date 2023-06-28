/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MapPartitions;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject;

@Slf4j
public class MergeIntoOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<MapPartitions> {

  public MergeIntoOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return getMergeIntoOutputRelation(x).isPresent();
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, MapPartitions x) {
    if (x.func().toString().contains("org.apache.spark.sql.delta.commands.MergeIntoCommand")) {
      Optional<LogicalPlan> outputRelationForMergeInto = getMergeIntoOutputRelation(x);

      if (outputRelationForMergeInto.isPresent()) {
        delegate(
                context.getOutputDatasetQueryPlanVisitors(),
                context.getOutputDatasetBuilders(),
                event)
            .applyOrElse(
                outputRelationForMergeInto.get(),
                ScalaConversionUtils.toScalaFn((lp) -> Collections.<OutputDataset>emptyList()))
            .stream()
            .collect(Collectors.toList());
      }
    }

    log.debug("MapPartitionsDatasetBuilder apply works only with MergeIntoCommand command on 3.1");
    return Collections.emptyList();
  }

  // TODO: write some unit test for this

  public static Optional<LogicalPlan> getMergeIntoOutputRelation(LogicalPlan plan) {
    return getDeltaMergeIntoOutputRelation(plan);
  }

  /**
   * In case of delta `MERGE INTO`, the command is translated into following plan:
   *
   * <pre>
   * == Optimized Logical Plan ==
   * SerializeFromObject ...
   * +- MapPartitions org.apache.spark.sql.delta.commands.MergeIntoCommand$$Lambda$ ...
   *    +- DeserializeToObject createexternalrow(a#1055L, b#1056.toString, _source_row_present_#1345
   *        +- Join FullOuter, (a#815L = a#819L)
   *            :- Project [a#819L, b#820, UDF() AS _source_row_present_#1052]
   *              : +- Relation[a#819L,b#820] parquet
   *            +- Project [a#815L, b#816, true AS _target_row_present_#1058]
   *                +- Relation[a#815L,b#816] parquet
   * </pre>
   *
   * The code below checks if this is the case. It checks if there is a full outer join with a
   * joined projection containing field `_target_row_present_`. If so, it extracts output dataset
   * from that projection.
   */
  public static Optional<LogicalPlan> getDeltaMergeIntoOutputRelation(LogicalPlan plan) {
    return Optional.of(plan)
        .filter(p -> p instanceof SerializeFromObject)
        .filter(p -> p.children().last() instanceof MapPartitions)
        .map(p -> (MapPartitions) p.children().last())
        .filter(
            mp ->
                mp.func()
                    .toString()
                    .contains("org.apache.spark.sql.delta.commands.MergeIntoCommand"))
        .filter(
            mp -> mp.children().size() > 0 && mp.children().last() instanceof DeserializeToObject)
        .map(mp -> (DeserializeToObject) mp.children().last())
        .filter(mp -> mp.children().size() > 0 && mp.children().last() instanceof Join)
        .map(mp -> (Join) mp.children().last())
        .filter(join -> join.joinType().toString().equalsIgnoreCase(JoinType.FULLOUTER.toString()))
        .filter(join -> join.children() != null)
        .filter(join -> join.children().size() == 2)
        .filter(join -> join.children().apply(0).output() != null)
        .filter(join -> join.children().apply(1).output() != null)
        .filter(join -> join.children().apply(0).output().size() > 0)
        .filter(join -> join.children().apply(0).output().size() > 1)
        .filter(
            join ->
                join.children().apply(0).output().last().name().startsWith("_source_row_present_"))
        .filter(
            join ->
                join.children().apply(1).output().last().name().startsWith("_target_row_present"))
        .map(join -> join.children().last())
        .filter(n -> n instanceof Project)
        .map(Project.class::cast)
        .filter(project -> project.children().size() > 0)
        .map(project -> project.children().last());
  }
}
