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

# Use an official OpenJDK runtime as a parent image
From registry.corp.kuaishou.com/kuaishou/kuaishou-base-images:supervisor-jdk1.8.0_181-v1
ARG PRODUCT_ENV
ARG JOB_ID
ARG TASK_ID
MAINTAINER mingliang@kuaishou.com
RUN curl -o /update_build.sh http://files.kuaishou.internal:8901/data/repo/sources/kuaishou/public/private_clouds/7330/update_build.sh && sh /update_build.sh && rm -f /update_build.sh
COPY . /home/web_server/kuaishou-runner/webapps
WORKDIR /home/web_server
# 配置环境变量
ENV LANG en_US.UTF-8
ENV JAVA_VERSION 11
ENV JAVA_HOME /home/web_server/jdk
ENV JRE_HOME $JAVA_HOME/jre
ENV CLASSPATH .:$JAVA_HOME/lib:$JRE_HOME/lib
ENV PATH $PATH:$JAVA_HOME/bin
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

RUN tar -xvzf /home/web_server/kuaishou-runner/webapps/paimon-rest-server-dist.tar.gz -C /home/web_server/kuaishou-runner/webapps \
    && rm -f /home/web_server/kuaishou-runner/webapps/paimon-rest-server-dist.tar.gz
RUN ln -s /home/web_server/kuaishou-runner/webapps/paimon-rest-server/bin/start.sh /home/web_server/kuaishou-runner/webapps/start.sh

CMD sh -x /home/web_server/kuaishou-runner/sbin/d_control