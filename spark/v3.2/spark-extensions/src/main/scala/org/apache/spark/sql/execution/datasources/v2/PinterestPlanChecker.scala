/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.BinaryComparison
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType

/**
 * This strategy does not modify the spark plan but throws an exception if it detects a filter of binary comparison
 * between string type partition column and date type literal.
 */
case class PinterestPlanChecker(spark: SparkSession) extends Strategy with PredicateHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Filter(condition, child: DataSourceV2ScanRelation) =>
      condition.containsChild.foreach {
        case _@BinaryComparison(_@Literal(value, DateType), Cast(fromExp: AttributeReference, DateType, _, _)) =>
          throwExceptionIfNotPushDown(fromExp, value, child)
        case _@BinaryComparison(Cast(fromExp: AttributeReference, DateType, _, _), _@Literal(value, DateType)) =>
          throwExceptionIfNotPushDown(fromExp, value, child)
        case _ =>
      }
      Nil

    case _ => Nil
  }

  private def throwExceptionIfNotPushDown(fromExp: AttributeReference,
                                          value: Any, child: DataSourceV2ScanRelation): Unit = {
    if (fromExp.dataType == StringType && value != null && isIcebergRelation(child.relation)) {
      val table = child.relation.table
      if (table.partitioning().toStream.exists(p => p.isInstanceOf[IdentityTransform] &&
        p.asInstanceOf[IdentityTransform].ref.fieldNames().size == 1 &&
        p.asInstanceOf[IdentityTransform].ref.fieldNames()(0).equals(fromExp.name))) {
        throw new Exception("Detected StringType partition filter comparison with DateType causing filter not" +
          " able to be pushed down to scan operator. Please follow <link> to fix the issue.")
      }
    }
  }
}
