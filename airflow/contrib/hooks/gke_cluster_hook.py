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
# # Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import time

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient import discovery


class GoogleKubernetesEngineClusterHook(GoogleCloudBaseHook):
    """
    Interacts with a Google Kubernetes Enginer Cluster using Google Cloud Base
    Hook class.
    """

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        super(GoogleKubernetesEngineClusterHook, self).__init__(gcp_conn_id,
                                                                delegate_to=delegate_to)

    def get_conn(self):
        """
        Returns a Google Kubernetes Engine service object.
        """
        http_authorized = self._authorize()
        return discovery.build('container', 'v1', http=http_authorized)

    def _wait_for_response(self, service, operation_name):
        self.log.info("Waiting for OPERATION_NAME %s" % operation_name)
        time.sleep(15)
        while True:
            response = service.projects().zones().operations().get(
                name=operation_name
            ).execute()
            if 'done' in response and response['done']:
                if 'error' in response:
                    raise Exception(str(response['error']))
                else:
                    return
            time.sleep(15)

    def delete_cluster(self, project_id, cluster_name, zone):
        service = self.get_conn()
        request = service.projects().zones().clusters().delete(
            projectId=project_id, zone=zone, clusterId=cluster_name)

        self.log.info(
            "Deleting CLUSTER_NAME %s from ZONE %s from PROJECT_ID %s" % (
                cluster_name, zone, project_id))

        response = request.execute()
        self._wait_for_response(service, response['name'])
