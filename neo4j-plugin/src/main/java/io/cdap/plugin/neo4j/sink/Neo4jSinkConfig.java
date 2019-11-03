/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.neo4j.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.neo4j.Neo4jConstants;

/**
 * Batch source to read from Neo4j.
 */
public class Neo4jSinkConfig extends PluginConfig {

  public static final String NAME_OUTPUT_QUERY = "outputQuery";

  @Name(Neo4jConstants.NAME_REFERENCE_NAME)
  @Description("")
  private String referenceName;

  @Macro
  @Name(Neo4jConstants.NAME_HOST_STRING)
  @Description("Neo4j database host.")
  private String neo4jHost;

  @Macro
  @Name(Neo4jConstants.NAME_PORT_STRING)
  @Description("Neo4j database port.")
  private int neo4jPort;

  @Name(Neo4jConstants.NAME_USERNAME)
  @Description("")
  private String username;

  @Name(Neo4jConstants.NAME_PASSWORD)
  @Description("")
  private String password;

  @Name(NAME_OUTPUT_QUERY)
  @Description("")
  private String outputQuery;

  public Neo4jSinkConfig(String referenceName, String neo4jHost, int neo4jPort, String username, String password,
                           String outputQuery) {
    this.referenceName = referenceName;
    this.neo4jHost = neo4jHost;
    this.neo4jPort = neo4jPort;
    this.username = username;
    this.password = password;
    this.outputQuery = outputQuery;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getNeo4jHost() {
    return neo4jHost;
  }

  public int getNeo4jPort() {
    return neo4jPort;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getOutputQuery() {
    return outputQuery;
  }

  public String getConnectionString() {
    return String.format(Neo4jConstants.NEO4J_CONNECTION_STRING_FORMAT, getNeo4jHost(), getNeo4jPort(),
                         getUsername(), getPassword());
  }

  public void validate(FailureCollector collector) {

  }
}
