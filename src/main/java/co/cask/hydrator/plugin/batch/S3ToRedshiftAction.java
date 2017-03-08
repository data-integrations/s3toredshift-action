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
package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.base.Strings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * S3ToRedshift Action Plugin - Loads the data from S3 into Redshift.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("S3ToRedshift")
@Description("S3ToRedshift Action that will load the data from AWS S3 bucket into the AWS Redshift table.")
public class S3ToRedshiftAction extends Action {
  private final S3ToRedshiftConfig config;

  public S3ToRedshiftAction(S3ToRedshiftConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    config.checkKeysAndRoleForConnection();
  }

  @Override
  public void run(ActionContext actionContext) throws Exception {
    config.checkKeysAndRoleForConnection();
    Connection connection = null;
    Statement statement = null;
    try {
      Class.forName("com.amazon.redshift.jdbc4.Driver");
      //Open a connection and define properties.
      Properties props = new Properties();
      props.setProperty("user", config.masterUser);
      props.setProperty("password", config.masterPassword);
      connection = DriverManager.getConnection(config.clusterDbUrl, props);
      statement = connection.createStatement();

      String commandToExecute = buildCopyCommand();
      statement.executeUpdate(commandToExecute);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
        String.format("Could not load an Amazon Redshift JDBC driver. %s", e.getMessage()));
    } catch (SQLException e) {
      throw new IllegalArgumentException(
        String.format("Error while loading the data from S3 bucket to Redshift. %s", e.getMessage()));
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        throw new IllegalArgumentException(
          String.format("Error while closing the connection. %s", e.getMessage()));
      }
    }
  }

  /**
   * Builds and returns the copy command as per the provided properties, to load the data from AWS S3 to Redshift table.
   *
   * @return copy command
   */
  private String buildCopyCommand() {
    StringBuilder copyCommand = new StringBuilder();
    copyCommand.append("copy").append(" ");
    copyCommand.append(config.tableName);

    // Check and append if column list is present.
    if (!Strings.isNullOrEmpty(config.listOfColumns)) {
      copyCommand.append("(");
      copyCommand.append(config.listOfColumns);
      copyCommand.append(")");
    }
    copyCommand.append(" ").append("from").append(" ").append("'");
    copyCommand.append(config.s3DataPath);
    copyCommand.append("'");

    // Add credentials for connection.
    copyCommand.append(" ").append("credentials").append(" ");
    // Check authentication is using keys or role.
    if (!(Strings.isNullOrEmpty(config.accessKey) && Strings.isNullOrEmpty(config.secretAccessKey))) {
      copyCommand.append("'").append("aws_access_key_id").append("=");
      copyCommand.append(config.accessKey);
      copyCommand.append(";").append("aws_secret_access_key").append("=");
      copyCommand.append(config.secretAccessKey);
      copyCommand.append("'");
    } else {
      copyCommand.append("'").append("aws_iam_role").append("=");
      copyCommand.append(config.iamRole);
      copyCommand.append("'");
    }

    // Check if region is present.
    if (!Strings.isNullOrEmpty(config.s3Region)) {
      copyCommand.append(" ").append("region").append(" ").append("'");
      copyCommand.append(config.s3Region);
      copyCommand.append("'");
    }
    // Set the format as avro.
    // TODO: [HYDRATOR-1392] Support other formats other than avro while loading the data from S3 to Redshift.
    copyCommand.append(" ").append("format as avro 'auto'").append(";");
    return copyCommand.toString();
  }

  /**
   * Config class for S3ToRedshiftAction.
   */
  public static class S3ToRedshiftConfig extends PluginConfig {

    @Description("Access key for AWS S3 to connect to. Either provide 'Keys(Access and Secret Access keys)' or 'IAM " +
      "Role' for connecting to AWS S3 bucket. (Macro-enabled)")
    @Nullable
    @Macro
    private String accessKey;

    @Description("Secret access key for AWS S3 to connect to. Either provide 'Keys(Access and Secret Access keys)' or" +
      " 'IAM Role' for connecting to AWS S3 bucket. (Macro-enabled)")
    @Nullable
    @Macro
    private String secretAccessKey;

    @Description("IAM Role for AWS S3 to connect to. This can only be used if the cluster is hosted on AWS servers. " +
      "Either provide 'Keys(Access and Secret Access keys)' or 'IAM Role' for connecting to AWS S3 bucket. " +
      "(Macro-enabled)")
    @Nullable
    @Macro
    private String iamRole;

    @Description("The region for AWS S3 to connect to. If not specified, then plugin will consider that S3 bucket is " +
      "in the same region as of the Redshift cluster. (Macro-enabled)")
    @Nullable
    @Macro
    private String s3Region;

    @Description("The S3 path of the bucket where the data is stored and will be loaded into the Redshift table. For " +
      "example, 's3://bucket-name/test/' or 's3://bucket-name/test/2017-02-22/'(will load files present in specific " +
      "directory) or 's3://bucket-name/test'(will load the files having prefix ``test``) or " +
      "'s3://bucket-name/test/2017-02-22'(will load files from ``test`` directory having prefix ``2017-02-22``). " +
      "(Macro-enabled)")
    @Macro
    private String s3DataPath;

    @Description("The JDBC Redshift database URL for Redshift cluster, where the table is present. For example, " +
      "'jdbc:redshift://x.y.us-west-2.redshift.amazonaws.com:5439/dev'. (Macro-enabled)")
    @Macro
    private String clusterDbUrl;

    @Description("Master user for the Redshift cluster to connect to. (Macro-enabled)")
    @Macro
    private String masterUser;

    @Description("Master password for Redshift cluster to connect to. (Macro-enabled)")
    @Macro
    private String masterPassword;

    @Description("The Redshift table name where the data from the S3 bucket will be loaded. (Macro-enabled)")
    @Macro
    private String tableName;

    @Description("Comma-separated list of the Redshift table column names to load the specific columns from S3 bucket" +
      ". If not provided, then all the columns from S3 will be loaded into the Redshift table. (Macro-enabled)")
    @Nullable
    @Macro
    private String listOfColumns;

    public S3ToRedshiftConfig(@Nullable String accessKey, @Nullable String secretAccessKey, @Nullable String iamRole,
                              @Nullable String s3Region, String s3DataPath, String clusterDbUrl, String masterUser,
                              String masterPassword, String tableName, @Nullable String listOfColumns) {
      this.accessKey = accessKey;
      this.secretAccessKey = secretAccessKey;
      this.iamRole = iamRole;
      this.s3Region = s3Region;
      this.s3DataPath = s3DataPath;
      this.clusterDbUrl = clusterDbUrl;
      this.masterUser = masterUser;
      this.masterPassword = masterPassword;
      this.tableName = tableName;
      this.listOfColumns = listOfColumns;
    }

    /**
     * Checks whether both the keys and role is present or empty at the same time, for connecting to S3 bucket.
     */
    private void checkKeysAndRoleForConnection() {
      if (!Strings.isNullOrEmpty(iamRole) || this.containsMacro("iamRole")) {
        if (!((Strings.isNullOrEmpty(accessKey) && !this.containsMacro("accessKey")) &&
          (Strings.isNullOrEmpty(secretAccessKey) && !this.containsMacro("secretAccessKey")))) {
          throw new IllegalArgumentException("Both configurations 'Keys(Access and Secret Access keys)' and 'IAM " +
                                               "Role' can not be provided at the same time. Either provide the 'Keys" +
                                               "(Access and Secret Access keys)' or 'IAM Role' for connecting to S3 " +
                                               "bucket.");
        }
      }

      if (Strings.isNullOrEmpty(iamRole)) {
        if (!((!Strings.isNullOrEmpty(accessKey) || this.containsMacro("accessKey")) &&
          (!Strings.isNullOrEmpty(secretAccessKey) || this.containsMacro("secretAccessKey")))) {
          throw new IllegalArgumentException("Both configurations 'Keys(Access and Secret Access keys)' and 'IAM " +
                                               "Role' can not be empty at the same time. Either provide the 'Keys" +
                                               "(Access and Secret Access keys)' or 'IAM Role' for connecting to S3 " +
                                               "bucket.");
        }
      }
    }
  }
}
