/*
 * Copyright © 2019 CDAP
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
package io.cdap.plugin;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.plugin.batch.S3ToRedshiftAction;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Tests for S3ToRedshiftConfig.
 */
public class S3ToRedshiftConfigTest {
  private static final String IAM_ROLE = "iamRole";
  private static final String ACCESS_KEY = "accessKey";
  private static final String SECRET_ACCESS_KEY = "secretAccessKey";

  @Test
  public void testIfBothKeysAndRoleIsNotPresent() {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("", "", "", "", "s3://test-bucket/test/2017-02-22",
                                            "jdbc:redshift://x.y.us-east-1.redshift.amazonaws.com:5439/dev",
                                            "masterUser", "masterPassword", "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    new S3ToRedshiftAction(config).configurePipeline(configurer);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(IAM_ROLE, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(1)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(SECRET_ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(2)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testIfEitherKeyAndRoleIsNotPresent() {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("", "testaccesskey", "", "", "s3://test-bucket/test/2017-02-22",
                                            "jdbc:redshift://x.y.us-east-1.redshift.amazonaws.com:5439/dev",
                                            "masterUser", "masterPassword", "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    new S3ToRedshiftAction(config).configurePipeline(configurer);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(IAM_ROLE, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(1)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(SECRET_ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(2)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testIfBothKeysAndRoleIsPresent() {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("testAccessKey", "testSecretAccessKey", "testIamRole", "",
                                            "s3://test-bucket/test/2017-02-22", "jdbc:redshift://x.y.us-east-1" +
                                              ".redshift.amazonaws.com:5439/dev", "masterUser", "masterPassword",
                                            "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    new S3ToRedshiftAction(config).configurePipeline(configurer);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(IAM_ROLE, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(1)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(SECRET_ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(2)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testIfEitherKeyAndRoleIsPresent() {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("testAccessKey", "testSecretAccessKey", "testIamRole", "",
                                            "s3://test-bucket/test/2017-02-22", "jdbc:redshift://x.y.us-east-1" +
                                              ".redshift.amazonaws.com:5439/dev", "masterUser", "masterPassword",
                                            "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    new S3ToRedshiftAction(config).configurePipeline(configurer);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(IAM_ROLE, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(1)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(SECRET_ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(2)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testIfBothKeysAndRolePresentAsMacro() {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("${accessKey}", "${secretAccessKey}", "${iamRole}", "",
                                            "s3://test-bucket/test/2017-02-22", "jdbc:redshift://x.y.us-east-1" +
                                              ".redshift.amazonaws.com:5439/dev", "masterUser", "masterPassword",
                                            "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    new S3ToRedshiftAction(config).configurePipeline(configurer);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(IAM_ROLE, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(1)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(SECRET_ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(2)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testIfKeysAndRoleIsPresentAsMacro() {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("testAccessKey", "testSecretAccessKey", "${iamRole}", "",
                                            "s3://test-bucket/test/2017-02-22", "jdbc:redshift://x.y.us-east-1" +
                                              ".redshift.amazonaws.com:5439/dev", "masterUser", "masterPassword",
                                            "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    FailureCollector collector = configurer.getStageConfigurer().getFailureCollector();
    new S3ToRedshiftAction(config).configurePipeline(configurer);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(IAM_ROLE, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(1)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(SECRET_ACCESS_KEY, collector.getValidationFailures().get(0).getCauses().get(2)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
