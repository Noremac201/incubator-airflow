# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from google.cloud import container_v1, exceptions
from airflow.hooks.base_hook import BaseHook
from google.cloud.container_v1.gapic.enums import Operation

import time


class GKEClusterHook(BaseHook):

    def __init__(self, project_id, location, cluster_id):
        self.project_id = project_id
        self.location = location
        self.cluster_id = cluster_id
        self.client = container_v1.ClusterManagerClient()

    def _wait_for_operation(self, operation):
        self.log.info("Waiting for OPERATION_NAME %s" % operation.name)
        time.sleep(15)
        while operation.status != Operation.Status.DONE:
            if operation.status == Operation.Status.RUNNING or operation.status == Operation.Status.PENDING:
                time.sleep(15)
            else:
                raise exceptions.GoogleCloudError(
                    "Operation has failed with status: %s" % operation.status)
            # To update status of operation
            operation = self.get_operation(operation.name)
        return

    def delete_cluster(self):
        """
            Deletes the cluster, including the Kubernetes endpoint and all
            worker nodes. Firewalls and routes that were configured during
            cluster creation are also deleted. Other Google Compute Engine
            resources that might be in use by the cluster (e.g. load balancer
            resources) will not be deleted if they weren’t present at the
            initial create time.
        """
        self.log.info("Deleting (project_id=%s, zone=%s, cluster_id=%s)" %
                      self.project_id, self.location, self.cluster_id)
        delete_op = self.client.delete_cluster(project_id=self.project_id,
                                               zone=self.location,
                                               cluster_id=self.cluster_id)
        self._wait_for_operation(delete_op)

    def get_operation(self, operation_name):
        return self.client.get_operation(project_id=self.project_id,
                                         zone=self.location,
                                         operation_id=operation_name)
