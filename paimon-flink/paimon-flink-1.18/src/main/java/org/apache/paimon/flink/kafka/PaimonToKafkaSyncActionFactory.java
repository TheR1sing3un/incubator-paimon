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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link PaimonToKafkaSyncAction}. */
public class PaimonToKafkaSyncActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "paimon_to_kafka_sync";
    private static final String KAFKA_CONF = "kafka_conf";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String database = params.getRequired(DATABASE);
        String table = params.getRequired(TABLE);
        Map<String, String> catalogConfig = catalogConfigMap(params);
        Map<String, String> tableConfig = optionalConfigMap(params, TABLE_CONF);
        Map<String, String> kafkaConfig = optionalConfigMap(params, KAFKA_CONF);

        PaimonToKafkaSyncAction action =
                new PaimonToKafkaSyncAction(
                        database, table, catalogConfig, tableConfig, kafkaConfig);
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"paimon_to_kafka_sync\" synchronizes a Paimon table's row data to Kafka as flat JSON.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  paimon_to_kafka_sync --warehouse <warehouse_path> --database <database>"
                        + " --table <table> [--table_conf <key>=<value>] ... [--kafka_conf <key>=<value>] ...");
        System.out.println();

        System.out.println("Required kafka_conf:");
        System.out.println("  topic                             Target Kafka topic");
        System.out.println("  properties.bootstrap.servers      Kafka broker addresses");
        System.out.println();

        System.out.println("Optional table_conf:");
        System.out.println(
                "  consumer-id                       "
                        + "Consumer id for recording consumption offset (enables restart resume)");
        System.out.println(
                "  consumer.expiration-time           "
                        + "Expiration time for consumer (required when consumer-id is set)");
        System.out.println(
                "  scan.mode                         "
                        + "Scan startup mode (default, latest, latest-full, from-timestamp, etc.)");
        System.out.println();

        System.out.println("Optional kafka_conf:");
        System.out.println(
                "  monitor-interval                  "
                        + "Snapshot scan interval in milliseconds (default: 10000)");
        System.out.println(
                "  delivery-guarantee                "
                        + "Delivery guarantee: exactly-once, at-least-once (default), none");
        System.out.println(
                "  transactional-id-prefix           "
                        + "Kafka transactional id prefix (required for exactly-once)");
        System.out.println(
                "  properties.*                      " + "Additional Kafka producer properties");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  paimon_to_kafka_sync --warehouse hdfs:///warehouse --database mydb --table mytable \\");
        System.out.println("    --kafka_conf topic=output_topic \\");
        System.out.println("    --kafka_conf properties.bootstrap.servers=localhost:9092 \\");
        System.out.println("    --table_conf consumer-id=myConsumer \\");
        System.out.println("    --table_conf consumer.expiration-time=24h");
    }
}
