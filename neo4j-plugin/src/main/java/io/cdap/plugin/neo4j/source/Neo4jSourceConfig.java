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

package io.cdap.plugin.neo4j.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * Batch source to read from Neo4j.
 */
public class Neo4jSourceConfig extends PluginConfig {

  public static final String NEO4J_CONNECTION_STRING_FORMAT = "jdbc:neo4j:bolt://%s:%s/?username=%s,password=%s";

  public static final String REFERENCE_NAME = "referenceName";
  public static final String NAME_HOST_STRING = "neo4jHost";
  public static final String NAME_PORT_STRING = "neo4jPort";
  public static final String NAME_USERNAME = "username";
  public static final String NAME_PASSWORD = "password";
  public static final String NAME_INPUT_QUERY = "inputQuery";
  public static final String NAME_SPLIT_NUM = "splitNum";
  public static final String NAME_ORDER_BY = "orderBy";

  @Name(REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  private String referenceName;

  @Name(NAME_HOST_STRING)
  @Description("Neo4j database host.")
  private String neo4jHost;

  @Name(NAME_PORT_STRING)
  @Description("Neo4j database port.")
  private int neo4jPort;

  @Name(NAME_USERNAME)
  @Description("User to use to connect to the Neo4j database.")
  private String username;

  @Name(NAME_PASSWORD)
  @Description("Password to use to connect to the Neo4j database.")
  private String password;

  @Name(NAME_INPUT_QUERY)
  @Description("The query to use to import data from the Neo4j database. " +
    "Query example: 'MATCH (n:Label) RETURN n.property_1, n.property_2'.")
  private String inputQuery;

  @Nullable
  @Name(NAME_SPLIT_NUM)
  @Description("The number of splits to generate. If set to one, the orderBy is not needed.")
  private Integer splitNum;

  @Nullable
  @Name(NAME_ORDER_BY)
  @Description("Field Name which will be used for ordering during splits generation. " +
    "This is required unless numSplits is set to one.")
  private String orderBy;

  public Neo4jSourceConfig(String referenceName, String neo4jHost, int neo4jPort, String username, String password,
                           String inputQuery, int splitNum, @Nullable String orderBy) {
    this.referenceName = referenceName;
    this.neo4jHost = neo4jHost;
    this.neo4jPort = neo4jPort;
    this.username = username;
    this.password = password;
    this.inputQuery = inputQuery;
    this.splitNum = splitNum;
    this.orderBy = orderBy;
  }

  private Neo4jSourceConfig(Builder builder) {
    referenceName = builder.referenceName;
    neo4jHost = builder.neo4jHost;
    neo4jPort = builder.neo4jPort;
    username = builder.username;
    password = builder.password;
    inputQuery = builder.inputQuery;
    splitNum = builder.splitNum;
    orderBy = builder.orderBy;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Neo4jSourceConfig copy) {
    return builder()
      .setReferenceName(copy.referenceName)
      .setNeo4jHost(copy.neo4jHost)
      .setNeo4jPort(copy.neo4jPort)
      .setUsername(copy.username)
      .setPassword(copy.password)
      .setInputQuery(copy.inputQuery)
      .setSplitNum(copy.splitNum)
      .setOrderBy(copy.orderBy);
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

  public String getInputQuery() {
    return inputQuery;
  }

  public int getSplitNum() {
    return splitNum == null ? 1 : splitNum;
  }

  @Nullable
  public String getOrderBy() {
    return orderBy;
  }

  public String getConnectionString() {
    return String.format(Neo4jSourceConfig.NEO4J_CONNECTION_STRING_FORMAT, getNeo4jHost(), getNeo4jPort(),
                         getUsername(), getPassword());
  }

  public void validate() {
    String[] unavailableQueryKeywords = new String[] {"UNWIND", "CREATE", "DELETE", "SET", "REMOVE", "MERGE"};
    String[] requiredQueryKeywords = new String[] {"MATCH", "RETURN"};

    if (Arrays.stream(unavailableQueryKeywords).parallel().anyMatch(inputQuery.toUpperCase()::contains)) {
      throw new IllegalArgumentException(
        String.format("The input request must not contain any of the following keywords: '%s'",
                      Arrays.toString(unavailableQueryKeywords)));
    }
    if (!Arrays.stream(requiredQueryKeywords).parallel().allMatch(inputQuery.toUpperCase()::contains)) {
      throw new IllegalArgumentException(
        String.format("The input request must contain following keywords: '%s'",
                      Arrays.toString(requiredQueryKeywords)));
    }
    if (getSplitNum() <= 0) {
      throw new IllegalArgumentException("Splits number must be greater than 0.");
    }

    if (getSplitNum() > 1 && orderBy == null) {
      throw new IllegalArgumentException("Order by field required if Splits number greater than 1.");
    }
  }


  public static final class Builder {
    private String referenceName;
    private String neo4jHost;
    private int neo4jPort;
    private String username;
    private String password;
    private String inputQuery;
    private Integer splitNum;
    private String orderBy;

    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setNeo4jHost(String neo4jHost) {
      this.neo4jHost = neo4jHost;
      return this;
    }

    public Builder setNeo4jPort(int neo4jPort) {
      this.neo4jPort = neo4jPort;
      return this;
    }

    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setInputQuery(String inputQuery) {
      this.inputQuery = inputQuery;
      return this;
    }

    public Builder setSplitNum(Integer splitNum) {
      this.splitNum = splitNum;
      return this;
    }

    public Builder setOrderBy(String orderBy) {
      this.orderBy = orderBy;
      return this;
    }

    public Neo4jSourceConfig build() {
      return new Neo4jSourceConfig(this);
    }
  }
}
