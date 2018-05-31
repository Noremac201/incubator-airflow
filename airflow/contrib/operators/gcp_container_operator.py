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
from google.api_core import retry
from google.api_core.exceptions import BadRequest

from airflow.contrib.hooks.gcp_container_hook import GKEClusterHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GKEClusterDeleteOperator(BaseOperator):
    template_fields = ['project_id', 'gcp_conn_id', 'location', 'api_version', 'body']

    @apply_defaults
    def __init__(self,
        project_id,
        location=None,
        gcp_conn_id='google_cloud_default',
        api_version='v2',
        body={},
        *args,
        **kwargs):
        super(GKEClusterDeleteOperator, self).__init__(*args, **kwargs)

        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.api_version = api_version
        self.body = body

    @retry.Retry()
    def execute(self, context):
        if not self.location:
            raise BadRequest("delete_cluster requires a location (zone)")
        if not self.body or not self.body['name']:
            raise BadRequest("Must specify name of cluster to delete in body dictionary")

        hook = GKEClusterHook(self.project_id, self.location, self.body['name'])
        hook.delete_cluster()
