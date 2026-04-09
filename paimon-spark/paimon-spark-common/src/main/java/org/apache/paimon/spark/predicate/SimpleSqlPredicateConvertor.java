/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.predicate;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Convert simple SQL WHERE expressions to Paimon {@link Predicate}s using Apache Calcite via
 * reflection.
 *
 * <p>Calcite classes are available at Spark runtime but not at compile time in paimon-spark-common,
 * so we access them via reflection. Unlike the Flink version, Spark does not hide Calcite in a
 * separate classloader — we simply use the thread context classloader.
 */
public class SimpleSqlPredicateConvertor {

    private final PredicateBuilder builder;
    private final RowType rowType;

    // Calcite class handles (loaded via reflection)
    private final Class<?> sqlParserClass;
    private final Class<?> sqlParserConfigClass;
    private final Class<?> lexClass;
    private final Class<?> sqlBasicCallClass;
    private final Class<?> sqlOperatorClass;
    private final Class<?> sqlBinaryOperatorClass;
    private final Class<?> sqlPostfixOperatorClass;
    private final Class<?> sqlPrefixOperatorClass;
    private final Class<?> sqlIdentifierClass;
    private final Class<?> sqlLiteralClass;
    private final Class<?> sqlNodeListClass;
    private final Class<?> sqlKindClass;

    // SqlKind enum values
    private final Object kindOr;
    private final Object kindAnd;
    private final Object kindEquals;
    private final Object kindNotEquals;
    private final Object kindLessThan;
    private final Object kindLessThanOrEqual;
    private final Object kindGreaterThan;
    private final Object kindGreaterThanOrEqual;
    private final Object kindIn;
    private final Object kindNotIn;
    private final Object kindIsNull;
    private final Object kindIsNotNull;
    private final Object kindNot;

    public SimpleSqlPredicateConvertor(RowType type) throws Exception {
        this.rowType = type;
        this.builder = new PredicateBuilder(type);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        this.sqlParserClass = cl.loadClass("org.apache.calcite.sql.parser.SqlParser");
        this.sqlParserConfigClass = cl.loadClass("org.apache.calcite.sql.parser.SqlParser$Config");
        this.lexClass = cl.loadClass("org.apache.calcite.config.Lex");
        this.sqlBasicCallClass = cl.loadClass("org.apache.calcite.sql.SqlBasicCall");
        this.sqlOperatorClass = cl.loadClass("org.apache.calcite.sql.SqlOperator");
        this.sqlBinaryOperatorClass = cl.loadClass("org.apache.calcite.sql.SqlBinaryOperator");
        this.sqlPostfixOperatorClass = cl.loadClass("org.apache.calcite.sql.SqlPostfixOperator");
        this.sqlPrefixOperatorClass = cl.loadClass("org.apache.calcite.sql.SqlPrefixOperator");
        this.sqlIdentifierClass = cl.loadClass("org.apache.calcite.sql.SqlIdentifier");
        this.sqlLiteralClass = cl.loadClass("org.apache.calcite.sql.SqlLiteral");
        this.sqlNodeListClass = cl.loadClass("org.apache.calcite.sql.SqlNodeList");
        this.sqlKindClass = cl.loadClass("org.apache.calcite.sql.SqlKind");

        this.kindOr = sqlKindClass.getField("OR").get(null);
        this.kindAnd = sqlKindClass.getField("AND").get(null);
        this.kindEquals = sqlKindClass.getField("EQUALS").get(null);
        this.kindNotEquals = sqlKindClass.getField("NOT_EQUALS").get(null);
        this.kindLessThan = sqlKindClass.getField("LESS_THAN").get(null);
        this.kindLessThanOrEqual = sqlKindClass.getField("LESS_THAN_OR_EQUAL").get(null);
        this.kindGreaterThan = sqlKindClass.getField("GREATER_THAN").get(null);
        this.kindGreaterThanOrEqual = sqlKindClass.getField("GREATER_THAN_OR_EQUAL").get(null);
        this.kindIn = sqlKindClass.getField("IN").get(null);
        this.kindNotIn = sqlKindClass.getField("NOT_IN").get(null);
        this.kindIsNull = sqlKindClass.getField("IS_NULL").get(null);
        this.kindIsNotNull = sqlKindClass.getField("IS_NOT_NULL").get(null);
        this.kindNot = sqlKindClass.getField("NOT").get(null);
    }

    public Predicate convertSqlToPredicate(String whereSql) throws Exception {
        // SqlParser.config()
        Object config = sqlParserClass.getMethod("config").invoke(null);
        // config.withLex(Lex.JAVA)
        Object lexJava = lexClass.getField("JAVA").get(null);
        config = config.getClass().getMethod("withLex", lexClass).invoke(config, lexJava);
        // SqlParser.create(sql, config)
        Object sqlParser =
                sqlParserClass
                        .getMethod("create", String.class, sqlParserConfigClass)
                        .invoke(null, whereSql, config);
        // sqlParser.parseExpression()
        Object sqlNode = sqlParserClass.getMethod("parseExpression").invoke(sqlParser);
        return convert(sqlNode);
    }

    private Predicate convert(Object sqlNode) throws Exception {
        // sqlBasicCall.getOperator()
        Object operator = sqlBasicCallClass.getMethod("getOperator").invoke(sqlNode);
        // operator.getKind()
        Object kind = sqlOperatorClass.getMethod("getKind").invoke(operator);

        if (sqlBinaryOperatorClass.isInstance(operator)) {
            List<?> operandList =
                    (List<?>) sqlBasicCallClass.getMethod("getOperandList").invoke(sqlNode);
            Object left = operandList.get(0);
            Object right = operandList.get(1);

            if (kind == kindOr) {
                return PredicateBuilder.or(convert(left), convert(right));
            } else if (kind == kindAnd) {
                return PredicateBuilder.and(convert(left), convert(right));
            } else if (kind == kindEquals) {
                return visitBiFunction(left, right, builder::equal, builder::equal);
            } else if (kind == kindNotEquals) {
                return visitBiFunction(left, right, builder::notEqual, builder::notEqual);
            } else if (kind == kindLessThan) {
                return visitBiFunction(left, right, builder::lessThan, builder::greaterThan);
            } else if (kind == kindLessThanOrEqual) {
                return visitBiFunction(left, right, builder::lessOrEqual, builder::greaterOrEqual);
            } else if (kind == kindGreaterThan) {
                return visitBiFunction(left, right, builder::greaterThan, builder::lessThan);
            } else if (kind == kindGreaterThanOrEqual) {
                return visitBiFunction(left, right, builder::greaterOrEqual, builder::lessOrEqual);
            } else if (kind == kindIn) {
                int index = getFieldIndex(left.toString());
                List<?> elementsList = getNodeList(right);
                List<Object> list = new ArrayList<>();
                for (Object node : elementsList) {
                    list.add(
                            TypeUtils.castFromString(
                                    toValue(node), rowType.getFieldTypes().get(index)));
                }
                return builder.in(index, list);
            } else if (kind == kindNotIn) {
                int index = getFieldIndex(left.toString());
                List<?> elementsList = getNodeList(right);
                List<Object> list = new ArrayList<>();
                for (Object node : elementsList) {
                    list.add(
                            TypeUtils.castFromString(
                                    toValue(node), rowType.getFieldTypes().get(index)));
                }
                return builder.in(index, list).negate().get();
            }
        } else if (sqlPostfixOperatorClass.isInstance(operator)) {
            Object child =
                    ((List<?>) sqlBasicCallClass.getMethod("getOperandList").invoke(sqlNode))
                            .get(0);
            if (kind == kindIsNull) {
                return builder.isNull(getFieldIndex(String.valueOf(child)));
            } else if (kind == kindIsNotNull) {
                return builder.isNotNull(getFieldIndex(String.valueOf(child)));
            }
        } else if (sqlPrefixOperatorClass.isInstance(operator)) {
            if (kind == kindNot) {
                Object child =
                        ((List<?>) sqlBasicCallClass.getMethod("getOperandList").invoke(sqlNode))
                                .get(0);
                return convert(child).negate().get();
            }
        }

        throw new UnsupportedOperationException(String.format("%s not been supported.", kind));
    }

    private Predicate visitBiFunction(
            Object left,
            Object right,
            BiFunction<Integer, Object, Predicate> visitLeft,
            BiFunction<Integer, Object, Predicate> visitRight)
            throws Exception {
        if (sqlIdentifierClass.isInstance(left) && sqlLiteralClass.isInstance(right)) {
            int index = getFieldIndex(String.valueOf(left));
            String value = toValue(right);
            DataType type = rowType.getFieldTypes().get(index);
            return visitLeft.apply(index, TypeUtils.castFromString(value, type));
        } else if (sqlIdentifierClass.isInstance(right) && sqlLiteralClass.isInstance(left)) {
            int index = getFieldIndex(right.toString());
            return visitRight.apply(
                    index,
                    TypeUtils.castFromString(toValue(left), rowType.getFieldTypes().get(index)));
        }

        throw new UnsupportedOperationException(
                String.format("%s or %s not been supported.", left, right));
    }

    private String toValue(Object sqlLiteral) throws Exception {
        return (String) sqlLiteralClass.getMethod("toValue").invoke(sqlLiteral);
    }

    @SuppressWarnings("unchecked")
    private List<?> getNodeList(Object sqlNodeList) throws Exception {
        return (List<?>) sqlNodeListClass.getMethod("getList").invoke(sqlNodeList);
    }

    private int getFieldIndex(String field) {
        int index = builder.indexOf(field);
        if (index == -1) {
            throw new RuntimeException(String.format("Field `%s` not found", field));
        }
        return index;
    }
}
