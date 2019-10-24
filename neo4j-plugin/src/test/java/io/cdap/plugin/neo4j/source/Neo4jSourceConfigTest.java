package io.cdap.plugin.neo4j.source;

import org.junit.Test;

public class Neo4jSourceConfigTest {

  private static final Neo4jSourceConfig VALID_CONFIG = new Neo4jSourceConfig(
    "ref_name",
    "localhost",
    7687,
    "user",
    "password",
    "MATCH (n:Test) RETURN n",
    1,
    null
  );

  @Test
  public void testCheckValidConfig() {
    VALID_CONFIG.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateQueryWithUnavailableKeywords() {
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setInputQuery("MERGE (robert:Critic) RETURN robert, labels(robert)")
      .build();
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateQueryWithoutRequiredKeywords() {
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setInputQuery("MATCH (robert:Critic)")
      .build();
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateInvalidSplitNumber() {
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setSplitNum(-2)
      .build();
    config.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateSplitNumberWithoutOrderBy() {
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setSplitNum(10)
      .build();
    config.validate();
  }

  @Test
  public void testValidateSplitNumberWithOrderBy() {
    Neo4jSourceConfig config = Neo4jSourceConfig.builder(VALID_CONFIG)
      .setSplitNum(10)
      .setOrderBy("n.id")
      .build();
    config.validate();
  }
}
