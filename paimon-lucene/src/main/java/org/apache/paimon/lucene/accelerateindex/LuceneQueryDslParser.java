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

package org.apache.paimon.lucene.accelerateindex;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Parses ES-style JSON DSL into Lucene {@link Query} objects.
 *
 * <p>Supported query types:
 *
 * <ul>
 *   <li>{@code match} - full-text match (tokenized via configured analyzer)
 *   <li>{@code match_phrase} - phrase match (all tokens must appear in order; optional slop)
 *   <li>{@code term} - exact match (no tokenization for text; exact value for numeric)
 *   <li>{@code prefix} - prefix match on text/keyword fields
 *   <li>{@code wildcard} - wildcard match ({@code *} = any sequence, {@code ?} = single char)
 *   <li>{@code regexp} - regular expression match on text/keyword fields
 *   <li>{@code fuzzy} - fuzzy match based on edit distance (optional fuzziness parameter)
 *   <li>{@code range} - numeric range query (supports gt/gte/lt/lte)
 *   <li>{@code must} - boolean AND
 *   <li>{@code should} - boolean OR
 *   <li>{@code must_not} - boolean NOT
 * </ul>
 *
 * <p>Example DSL:
 *
 * <pre>{@code
 * {
 *   "must": [
 *     {"match": {"contextEn": "Document"}},
 *     {"match_phrase": {"contextEn": "Save Document"}},
 *     {"term": {"version": "v5.8.0"}},
 *     {"prefix": {"version": "v5"}},
 *     {"wildcard": {"version": "v5.*"}},
 *     {"regexp": {"version": "v[0-9]+\\.8\\.0"}},
 *     {"fuzzy": {"version": "v5.8.o"}},
 *     {"range": {"score": {"gte": 5, "lt": 10}}}
 *   ]
 * }
 * }</pre>
 */
public class LuceneQueryDslParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Analyzer analyzer;
    private final Map<String, String> fieldTypeMap;

    public LuceneQueryDslParser() {
        this(new StandardAnalyzer(), Collections.emptyMap());
    }

    public LuceneQueryDslParser(Analyzer analyzer, Map<String, String> fieldTypeMap) {
        this.analyzer = analyzer;
        this.fieldTypeMap = fieldTypeMap;
    }

    /** Parses a JSON DSL string into a Lucene Query. */
    public Query parse(String dsl) throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(dsl);
        return parseNode(root);
    }

    private Query parseNode(JsonNode node) {
        // Top-level is an implicit bool query with must/should/must_not keys
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        boolean hasClause = false;

        if (node.has("must")) {
            for (JsonNode clause : node.get("must")) {
                builder.add(parseClause(clause), BooleanClause.Occur.MUST);
                hasClause = true;
            }
        }
        if (node.has("should")) {
            for (JsonNode clause : node.get("should")) {
                builder.add(parseClause(clause), BooleanClause.Occur.SHOULD);
                hasClause = true;
            }
        }
        if (node.has("must_not")) {
            for (JsonNode clause : node.get("must_not")) {
                builder.add(parseClause(clause), BooleanClause.Occur.MUST_NOT);
                hasClause = true;
            }
        }

        // If the node is a single clause (match or term), parse it directly
        if (!hasClause) {
            return parseClause(node);
        }

        return builder.build();
    }

    private Query parseClause(JsonNode clause) {
        if (clause.has("match")) {
            return parseMatch(clause.get("match"));
        } else if (clause.has("match_phrase")) {
            return parseMatchPhrase(clause.get("match_phrase"));
        } else if (clause.has("term")) {
            return parseTerm(clause.get("term"));
        } else if (clause.has("prefix")) {
            return parsePrefix(clause.get("prefix"));
        } else if (clause.has("wildcard")) {
            return parseWildcard(clause.get("wildcard"));
        } else if (clause.has("regexp")) {
            return parseRegexp(clause.get("regexp"));
        } else if (clause.has("fuzzy")) {
            return parseFuzzy(clause.get("fuzzy"));
        } else if (clause.has("range")) {
            return parseRange(clause.get("range"));
        } else {
            // Could be a nested bool
            return parseNode(clause);
        }
    }

    /**
     * Parses a "match" clause: tokenizes the value and creates a BooleanQuery of TermQueries with
     * SHOULD (any token matches).
     */
    private Query parseMatch(JsonNode matchNode) {
        Iterator<Map.Entry<String, JsonNode>> fields = matchNode.fields();
        if (!fields.hasNext()) {
            throw new IllegalArgumentException("Empty match clause");
        }
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldName = field.getKey();
        String text = field.getValue().asText();

        String fieldType = fieldTypeMap.getOrDefault(fieldName, "text");
        if (isNumericType(fieldType)) {
            throw new IllegalArgumentException(
                    "Cannot use 'match' on numeric field '"
                            + fieldName
                            + "'. Use 'term' or 'range' instead.");
        }

        // Tokenize using configured analyzer
        java.util.List<String> tokens = tokenize(fieldName, text);

        if (tokens.isEmpty()) {
            // No tokens produced — return a match-none query
            return new BooleanQuery.Builder().build();
        }
        if (tokens.size() == 1) {
            return new TermQuery(new Term(fieldName, tokens.get(0)));
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String token : tokens) {
            builder.add(new TermQuery(new Term(fieldName, token)), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    /** Parses a "term" clause: exact match. For numeric fields, uses Point exact queries. */
    private Query parseTerm(JsonNode termNode) {
        Iterator<Map.Entry<String, JsonNode>> fields = termNode.fields();
        if (!fields.hasNext()) {
            throw new IllegalArgumentException("Empty term clause");
        }
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldName = field.getKey();
        String fieldType = fieldTypeMap.getOrDefault(fieldName, "text");

        switch (fieldType) {
            case "int":
                return IntPoint.newExactQuery(fieldName, field.getValue().asInt());
            case "long":
                return LongPoint.newExactQuery(fieldName, field.getValue().asLong());
            case "float":
                return FloatPoint.newExactQuery(fieldName, (float) field.getValue().asDouble());
            case "double":
                return DoublePoint.newExactQuery(fieldName, field.getValue().asDouble());
            default:
                return new TermQuery(new Term(fieldName, field.getValue().asText()));
        }
    }

    /**
     * Parses a "match_phrase" clause: tokenizes the value and creates a PhraseQuery requiring all
     * tokens to appear in order. Supports optional {@code slop} parameter.
     *
     * <p>Example: {@code {"match_phrase": {"contextEn": "Save Document"}}} or {@code
     * {"match_phrase": {"contextEn": {"query": "Save Document", "slop": 1}}}}
     */
    private Query parseMatchPhrase(JsonNode matchPhraseNode) {
        Iterator<Map.Entry<String, JsonNode>> fields = matchPhraseNode.fields();
        if (!fields.hasNext()) {
            throw new IllegalArgumentException("Empty match_phrase clause");
        }
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldName = field.getKey();
        JsonNode valueNode = field.getValue();

        String text;
        int slop = 0;
        if (valueNode.isObject()) {
            text = valueNode.get("query").asText();
            if (valueNode.has("slop")) {
                slop = valueNode.get("slop").asInt();
            }
        } else {
            text = valueNode.asText();
        }

        String fieldType = fieldTypeMap.getOrDefault(fieldName, "text");
        if (isNumericType(fieldType)) {
            throw new IllegalArgumentException(
                    "Cannot use 'match_phrase' on numeric field '"
                            + fieldName
                            + "'. Use 'term' or 'range' instead.");
        }

        java.util.List<String> tokens = tokenize(fieldName, text);
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);
        for (int i = 0; i < tokens.size(); i++) {
            builder.add(new Term(fieldName, tokens.get(i)), i);
        }
        return builder.build();
    }

    /**
     * Parses a "prefix" clause: matches terms starting with the given prefix.
     *
     * <p>Example: {@code {"prefix": {"version": "v5"}}}
     */
    private Query parsePrefix(JsonNode prefixNode) {
        Map.Entry<String, JsonNode> field = extractSingleField(prefixNode, "prefix");
        rejectNumericField(field.getKey(), "prefix");
        return new PrefixQuery(new Term(field.getKey(), field.getValue().asText()));
    }

    /**
     * Parses a "wildcard" clause: {@code *} matches any sequence, {@code ?} matches single char.
     *
     * <p>Example: {@code {"wildcard": {"version": "v5.*"}}}
     */
    private Query parseWildcard(JsonNode wildcardNode) {
        Map.Entry<String, JsonNode> field = extractSingleField(wildcardNode, "wildcard");
        rejectNumericField(field.getKey(), "wildcard");
        return new WildcardQuery(new Term(field.getKey(), field.getValue().asText()));
    }

    /**
     * Parses a "regexp" clause: matches terms against a regular expression.
     *
     * <p>Example: {@code {"regexp": {"version": "v[0-9]+\\.8\\.0"}}}
     */
    private Query parseRegexp(JsonNode regexpNode) {
        Map.Entry<String, JsonNode> field = extractSingleField(regexpNode, "regexp");
        rejectNumericField(field.getKey(), "regexp");
        return new RegexpQuery(new Term(field.getKey(), field.getValue().asText()));
    }

    /**
     * Parses a "fuzzy" clause: matches terms within an edit distance (Levenshtein).
     *
     * <p>Simple form: {@code {"fuzzy": {"version": "v5.8.o"}}}
     *
     * <p>With fuzziness: {@code {"fuzzy": {"version": {"value": "v5.8.o", "fuzziness": 1}}}}
     */
    private Query parseFuzzy(JsonNode fuzzyNode) {
        Map.Entry<String, JsonNode> field = extractSingleField(fuzzyNode, "fuzzy");
        rejectNumericField(field.getKey(), "fuzzy");
        String fieldName = field.getKey();
        JsonNode valueNode = field.getValue();

        String text;
        int maxEdits = 2;
        if (valueNode.isObject()) {
            text = valueNode.get("value").asText();
            if (valueNode.has("fuzziness")) {
                maxEdits = valueNode.get("fuzziness").asInt();
            }
        } else {
            text = valueNode.asText();
        }

        return new FuzzyQuery(new Term(fieldName, text), maxEdits);
    }

    private Map.Entry<String, JsonNode> extractSingleField(JsonNode node, String clauseType) {
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        if (!fields.hasNext()) {
            throw new IllegalArgumentException("Empty " + clauseType + " clause");
        }
        return fields.next();
    }

    private void rejectNumericField(String fieldName, String clauseType) {
        String fieldType = fieldTypeMap.getOrDefault(fieldName, "text");
        if (isNumericType(fieldType)) {
            throw new IllegalArgumentException(
                    "Cannot use '"
                            + clauseType
                            + "' on numeric field '"
                            + fieldName
                            + "'. Use 'term' or 'range' instead.");
        }
    }

    /**
     * Parses a "range" clause for numeric fields. Supports gt/gte/lt/lte boundaries.
     *
     * <p>Example: {@code {"range": {"score": {"gte": 5, "lt": 10}}}}
     */
    private Query parseRange(JsonNode rangeNode) {
        Iterator<Map.Entry<String, JsonNode>> fields = rangeNode.fields();
        if (!fields.hasNext()) {
            throw new IllegalArgumentException("Empty range clause");
        }
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldName = field.getKey();
        JsonNode bounds = field.getValue();
        String fieldType = fieldTypeMap.getOrDefault(fieldName, "text");

        if (!isNumericType(fieldType)) {
            throw new IllegalArgumentException(
                    "Range queries only supported on numeric fields, got type '"
                            + fieldType
                            + "' for field '"
                            + fieldName
                            + "'");
        }

        switch (fieldType) {
            case "int":
                return buildIntRange(fieldName, bounds);
            case "long":
                return buildLongRange(fieldName, bounds);
            case "float":
                return buildFloatRange(fieldName, bounds);
            case "double":
                return buildDoubleRange(fieldName, bounds);
            default:
                throw new IllegalArgumentException("Unsupported numeric type: " + fieldType);
        }
    }

    private Query buildIntRange(String field, JsonNode bounds) {
        int lower = Integer.MIN_VALUE;
        int upper = Integer.MAX_VALUE;
        if (bounds.has("gte")) {
            lower = bounds.get("gte").asInt();
        } else if (bounds.has("gt")) {
            lower = Math.addExact(bounds.get("gt").asInt(), 1);
        }
        if (bounds.has("lte")) {
            upper = bounds.get("lte").asInt();
        } else if (bounds.has("lt")) {
            upper = Math.addExact(bounds.get("lt").asInt(), -1);
        }
        return IntPoint.newRangeQuery(field, lower, upper);
    }

    private Query buildLongRange(String field, JsonNode bounds) {
        long lower = Long.MIN_VALUE;
        long upper = Long.MAX_VALUE;
        if (bounds.has("gte")) {
            lower = bounds.get("gte").asLong();
        } else if (bounds.has("gt")) {
            lower = Math.addExact(bounds.get("gt").asLong(), 1);
        }
        if (bounds.has("lte")) {
            upper = bounds.get("lte").asLong();
        } else if (bounds.has("lt")) {
            upper = Math.addExact(bounds.get("lt").asLong(), -1);
        }
        return LongPoint.newRangeQuery(field, lower, upper);
    }

    private Query buildFloatRange(String field, JsonNode bounds) {
        float lower = Float.NEGATIVE_INFINITY;
        float upper = Float.POSITIVE_INFINITY;
        if (bounds.has("gte")) {
            lower = (float) bounds.get("gte").asDouble();
        } else if (bounds.has("gt")) {
            lower = Math.nextUp((float) bounds.get("gt").asDouble());
        }
        if (bounds.has("lte")) {
            upper = (float) bounds.get("lte").asDouble();
        } else if (bounds.has("lt")) {
            upper = Math.nextDown((float) bounds.get("lt").asDouble());
        }
        return FloatPoint.newRangeQuery(field, lower, upper);
    }

    private Query buildDoubleRange(String field, JsonNode bounds) {
        double lower = Double.NEGATIVE_INFINITY;
        double upper = Double.POSITIVE_INFINITY;
        if (bounds.has("gte")) {
            lower = bounds.get("gte").asDouble();
        } else if (bounds.has("gt")) {
            lower = Math.nextUp(bounds.get("gt").asDouble());
        }
        if (bounds.has("lte")) {
            upper = bounds.get("lte").asDouble();
        } else if (bounds.has("lt")) {
            upper = Math.nextDown(bounds.get("lt").asDouble());
        }
        return DoublePoint.newRangeQuery(field, lower, upper);
    }

    private static boolean isNumericType(String type) {
        return "int".equals(type)
                || "long".equals(type)
                || "float".equals(type)
                || "double".equals(type);
    }

    private java.util.List<String> tokenize(String fieldName, String text) {
        java.util.List<String> tokens = new java.util.ArrayList<>();
        try (org.apache.lucene.analysis.TokenStream ts = analyzer.tokenStream(fieldName, text)) {
            org.apache.lucene.analysis.tokenattributes.CharTermAttribute termAttr =
                    ts.addAttribute(
                            org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                tokens.add(termAttr.toString());
            }
            ts.end();
        } catch (IOException e) {
            throw new RuntimeException("Failed to tokenize text: " + text, e);
        }
        return tokens;
    }
}
