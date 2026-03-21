# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM mysql:8.0

# ============================================================
# Paimon REST Catalog Server Docker Image
# Includes: MySQL 8.0 + JDK 8 + Paimon REST Server
# ============================================================

# Install JDK 8 via Adoptium tarball (avoids Oracle Linux repo network issues)
RUN curl -fsSL https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u432-b06/OpenJDK8U-jdk_aarch64_linux_hotspot_8u432b06.tar.gz \
    | tar xz -C /opt && \
    ln -s /opt/jdk8u* /opt/java

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ---- MySQL configuration ----
ENV MYSQL_ROOT_PASSWORD=root123
ENV MYSQL_DATABASE=paimon_catalog
ENV MYSQL_USER=paimon
ENV MYSQL_PASSWORD=paimon123

# Copy schema init SQL (MySQL entrypoint auto-executes files in /docker-entrypoint-initdb.d/)
COPY sql/init.sql /docker-entrypoint-initdb.d/01-schema.sql
COPY sql/test-data.sql /docker-entrypoint-initdb.d/02-test-data.sql

# ---- Paimon REST Server ----
ENV SERVER_HOME=/opt/paimon-rest-server

# Copy server distribution
COPY lib/    ${SERVER_HOME}/lib/
COPY conf/   ${SERVER_HOME}/conf/
COPY bin/    ${SERVER_HOME}/bin/
RUN chmod +x ${SERVER_HOME}/bin/*.sh

# Copy docker-specific config (overrides default server.properties)
COPY conf/server-docker.properties ${SERVER_HOME}/conf/server.properties

# Logs directory
RUN mkdir -p ${SERVER_HOME}/logs

# ---- Entrypoint ----
# Save MySQL's original entrypoint before overwriting
RUN cp /usr/local/bin/docker-entrypoint.sh /usr/local/bin/mysql-entrypoint.sh
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/mysql-entrypoint.sh

EXPOSE 3306 26754

ENTRYPOINT ["docker-entrypoint.sh"]
