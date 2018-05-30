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


from airflow.contrib.hooks.gke_cluster_hook import \
    GoogleKubernetesEngineClusterHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleKubernetesEngineClusterDeleteOperator(BaseOperator):
    """
    Delete a Kubernetes Engine Cluster on Google Cloud.
    The operator will wait until the cluster is destroyed.

    :param cluster_name: The name of the cluster to delete. (templated)
    :type cluster_name: string
    :param project_id: The ID of the google cloud project in which
        the cluster runs. (templated)
    :type project_id: string
    :param zone: The zone of which the the cluster runs
    :type zone: string
    :param region: leave as 'global', might become relevant in the future. (templated)
    :type region: string
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    """

    template_fields = ['cluster_name', 'project_id', 'zone', 'region']

    @apply_defaults
    def __init__(self,
        cluster_name,
        project_id,
        zone,
        region='global',
        gcp_conn_id='google_cloud_default',
        delegate_to=None,
        *args,
        **kwargs):
        super(GoogleKubernetesEngineClusterDeleteOperator, self).__init__(*args,
                                                                          **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.cluster_name = cluster_name
        self.project_id = project_id
        self.region = region
        self.zone = zone

    def execute(self, context):
        hook = GoogleKubernetesEngineClusterHook(gcp_conn_id=self.gcp_conn_id,
                                                 delegate_to=self.delegate_to)
        hook.delete_cluster(project_id=self.project_id,
                            cluster_name=self.cluster_name,
                            zone=self.zone)
