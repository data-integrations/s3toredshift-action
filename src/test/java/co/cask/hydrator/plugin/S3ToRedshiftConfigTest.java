/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.hydrator.plugin;

import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.hydrator.plugin.batch.S3ToRedshiftAction;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Tests for S3ToRedshiftConfig.
 */
public class S3ToRedshiftConfigTest {

  @Test
  public void testIfBothKeysAndRoleIsNotPresent() throws Exception {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("", "", "", "", "s3://test-bucket/test/2017-02-22",
                                            "jdbc:redshift://x.y.us-east-1.redshift.amazonaws.com:5439/dev",
                                            "masterUser", "masterPassword", "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new S3ToRedshiftAction(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Both configurations 'Keys(Access and Secret Access keys)' and 'IAM Role' can not be " +
                            "empty at the same time. Either provide the 'Keys(Access and Secret Access keys)' or " +
                            "'IAM Role' for connecting to S3 bucket.", e.getMessage());
    }
  }

  @Test
  public void testIfEitherKeyAndRoleIsNotPresent() throws Exception {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("", "testaccesskey", "", "", "s3://test-bucket/test/2017-02-22",
                                            "jdbc:redshift://x.y.us-east-1.redshift.amazonaws.com:5439/dev",
                                            "masterUser", "masterPassword", "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new S3ToRedshiftAction(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Both configurations 'Keys(Access and Secret Access keys)' and 'IAM Role' can not be " +
                            "empty at the same time. Either provide the 'Keys(Access and Secret Access keys)' or " +
                            "'IAM Role' for connecting to S3 bucket.", e.getMessage());
    }
  }

  @Test
  public void testIfBothKeysAndRoleIsPresent() throws Exception {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("testAccessKey", "testSecretAccessKey", "testIamRole", "",
                                            "s3://test-bucket/test/2017-02-22", "jdbc:redshift://x.y.us-east-1" +
                                              ".redshift.amazonaws.com:5439/dev", "masterUser", "masterPassword",
                                            "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new S3ToRedshiftAction(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Both configurations 'Keys(Access and Secret Access keys)' and 'IAM Role' can not be " +
                            "provided at the same time. Either provide the 'Keys(Access and Secret Access keys)' or " +
                            "'IAM Role' for connecting to S3 bucket.", e.getMessage());
    }
  }

  @Test
  public void testIfEitherKeyAndRoleIsPresent() throws Exception {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("testAccessKey", "testSecretAccessKey", "testIamRole", "",
                                            "s3://test-bucket/test/2017-02-22", "jdbc:redshift://x.y.us-east-1" +
                                              ".redshift.amazonaws.com:5439/dev", "masterUser", "masterPassword",
                                            "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new S3ToRedshiftAction(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Both configurations 'Keys(Access and Secret Access keys)' and 'IAM Role' can not be " +
                            "provided at the same time. Either provide the 'Keys(Access and Secret Access keys)' or " +
                            "'IAM Role' for connecting to S3 bucket.", e.getMessage());
    }
  }

  @Test
  public void testIfBothKeysAndRolePresentAsMacro() throws Exception {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("${accessKey}", "${secretAccessKey}", "${iamRole}", "",
                                            "s3://test-bucket/test/2017-02-22", "jdbc:redshift://x.y.us-east-1" +
                                              ".redshift.amazonaws.com:5439/dev", "masterUser", "masterPassword",
                                            "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new S3ToRedshiftAction(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Both configurations 'Keys(Access and Secret Access keys)' and 'IAM Role' can not be " +
                            "provided at the same time. Either provide the 'Keys(Access and Secret Access keys)' or " +
                            "'IAM Role' for connecting to S3 bucket.", e.getMessage());
    }
  }

  @Test
  public void testIfKeysAndRoleIsPresentAsMacro() throws Exception {
    S3ToRedshiftAction.S3ToRedshiftConfig config = new
      S3ToRedshiftAction.S3ToRedshiftConfig("testAccessKey", "testSecretAccessKey", "${iamRole}", "",
                                            "s3://test-bucket/test/2017-02-22", "jdbc:redshift://x.y.us-east-1" +
                                              ".redshift.amazonaws.com:5439/dev", "masterUser", "masterPassword",
                                            "redshifttable", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new S3ToRedshiftAction(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Both configurations 'Keys(Access and Secret Access keys)' and 'IAM Role' can not be " +
                            "provided at the same time. Either provide the 'Keys(Access and Secret Access keys)' or " +
                            "'IAM Role' for connecting to S3 bucket.", e.getMessage());
    }
  }
}
