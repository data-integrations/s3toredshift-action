# S3ToRedshift Action Plugin

[![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)](https://docs.cdap.io/cdap/current/en/integrations/cask-market.html)
![cdap-action](https://cdap-users.herokuapp.com/assets/cdap-action.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)


Introduction
============
S3ToRedshift Action that will load the data from AWS S3 bucket into the AWS Redshift table.

Getting Started
===============

Prerequisites
--------------
CDAP version 3.5.x or higher.

Building Plugins
----------------
You get started with S3ToRedshift action plugin by building directly from the latest source code::

   git clone git@github.com:hydrator/s3toredshift-action.git
   cd s3toredshift-action
   mvn clean package

After the build completes, you will have a JAR for each plugin under each
``<plugin-name>/target/`` directory.

Deploying Plugins
-----------------
You can deploy a plugin using the CDAP CLI::

  > load artifact <target/plugin-jar> config-file <resources/plugin-config>

  > load artifact target/s3toredshift-action-plugin-<version>.jar \
         config-file target/s3toredshift-action-plugin-<version>.json

You can build without running tests: ``mvn clean install -DskipTests``

Limitations
-----------
- UI doesn't support schema's with hyphens (-), so the plugin currently transforms all the schemas with - into underscores (_). This change will be reverted after this is fixed: https://issues.cask.co/browse/HYDRATOR-1125

Mailing Lists
-------------
CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

IRC Channel
-----------
CDAP IRC Channel: #cdap on irc.freenode.net


License and Trademarks
======================

Copyright © 2019 CDAP

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.

.. |(Hydrator)| image:: http://cdao.io/wp-content/uploads/hydrator_logo_cdap1.png
