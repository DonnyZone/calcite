/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.enumerable.rex;

import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/** Implementor for the SQL {@code CAST} operator. */
class CastImplementor extends AbstractRexCallImplementor {

  CastImplementor() {
    super(NullPolicy.STRICT, false);
  }

  @Override String getVariableName() {
    return "cast";
  }

  @Override Expression implementSafe(final RexToLixTranslator translator,
      final RexCall call, final List<Expression> argValueList) {
    assert call.getOperands().size() == 1;
    final RelDataType sourceType = call.getOperands().get(0).getType();
    // Short-circuit if no cast is required
    if (call.getType().equals(sourceType)) {
      // No cast required, omit cast
      return argValueList.get(0);
    }
    final Expression argValue =
        RexImpTable.NullAs.NOT_POSSIBLE.handle(argValueList.get(0));
    final boolean nullable = false;
    final RelDataType targetType =
        nullifyType(
            translator.getTypeFactory(),
            call.getType(),
            nullable);
    Expression result = EnumUtils.doCast(
        translator.getTypeFactory(),
        translator.getRoot(),
        sourceType,
        targetType,
        argValue);
    if (targetType.getSqlTypeName() == SqlTypeName.BINARY
        || targetType.getSqlTypeName() == SqlTypeName.VARBINARY) {
      result = RexToLixTranslator.convert(result,
          translator.getTypeFactory().getJavaClass(targetType));
    }
    return result;
  }

  private RelDataType nullifyType(JavaTypeFactory typeFactory,
      RelDataType type, boolean nullable) {
    if (!nullable) {
      final Primitive primitive = javaPrimitive(type);
      if (primitive != null) {
        return typeFactory.createJavaType(primitive.primitiveClass);
      }
    }
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  private Primitive javaPrimitive(RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      return Primitive.ofBox(
          ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass());
    }
    return null;
  }
}

// End CastImplementor.java
