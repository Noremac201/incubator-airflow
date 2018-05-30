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

import unittest

from airflow.contrib.operators.gke_cluster_operator import \
    GoogleKubernetesEngineClusterDeleteOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TASK_ID = 'test-gke-cluster-operator'
CLUSTER_NAME = 'test-bucket'
TEST_PROJECT_ID = 'test-project'
ZONE = 'test-zone'


class GKEClusterDeleteTest(unittest.TestCase):
    @mock.patch(
        'airflow.contrib.operators.gke_cluster_operator.GoogleKubernetesEngineClusterDeleteOperator')
    def test_execute(self, mock_hook):
        operator = GoogleKubernetesEngineClusterDeleteOperator(
            task_id=TASK_ID,
            cluster_name=CLUSTER_NAME,
            zone=ZONE,
            project_id=TEST_PROJECT_ID
        )

        operator.execute(None)
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            task_id=TASK_ID, cluster_name=CLUSTER_NAME, zone=ZONE,
            project_id=TEST_PROJECT_ID)
