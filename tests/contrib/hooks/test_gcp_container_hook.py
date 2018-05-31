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
import unittest

from airflow.contrib.hooks.gcp_container_hook import GKEClusterHook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TASK_ID = 'test-gke-cluster-operator'
CLUSTER_NAME = 'test-cluster'
TEST_PROJECT_ID = 'test-project'
ZONE = 'test-zone'


class GKEClusterHookTest(unittest.TestCase):
    def setUp(self):
        with mock.patch.object(GKEClusterHook, "__init__", return_value=None):
            self.gke_hook = GKEClusterHook(None, None, None)
            self.gke_hook.project_id = TEST_PROJECT_ID
            self.gke_hook.location = ZONE
            self.gke_hook.cluster_id = CLUSTER_NAME
            self.gke_hook.client = mock.Mock()

    def test_get_operation(self):
        self.gke_hook.client.get_operation = mock.Mock()
        self.gke_hook.get_operation('TEST_OP')
        self.gke_hook.client.get_operation.assert_called_with(project_id=TEST_PROJECT_ID,
                                                              zone=ZONE,
                                                              operation_id='TEST_OP')

    @mock.patch(
        "airflow.contrib.hooks.gke_cluster_hook.GKEClusterHook._wait_for_operation")
    def test_delete_cluster(self, wait_mock):
        self.gke_hook.client.delete_cluster = mock.Mock()
        client_delete = self.gke_hook.client.delete_cluster
        self.gke_hook.delete_cluster()

        client_delete.assert_called_with(project_id=TEST_PROJECT_ID, zone=ZONE,
                                         cluster_id=CLUSTER_NAME)
        wait_mock.assert_called_with(client_delete.return_value)

    @mock.patch("time.sleep")
    def test_wait_for_response_done(self, time_mock):
        from google.cloud.container_v1.gapic.enums import Operation
        mock_op = mock.Mock()
        mock_op.status = Operation.Status.DONE
        self.gke_hook._wait_for_operation(mock_op)
        time_mock.assert_called_once()

    @mock.patch("time.sleep")
    def test_wait_for_response_exception(self, time_mock):
        from google.cloud.container_v1.gapic.enums import Operation
        from google.cloud.exceptions import GoogleCloudError

        mock_op = mock.Mock()
        mock_op.status = Operation.Status.ABORTING

        with self.assertRaises(GoogleCloudError):
            self.gke_hook._wait_for_operation(mock_op)
            time_mock.assert_called_once()

    @mock.patch("airflow.contrib.hooks.gke_cluster_hook.GKEClusterHook.get_operation")
    @mock.patch("time.sleep")
    def test_wait_for_response_running(self, time_mock, operation_mock):
        from google.cloud.container_v1.gapic.enums import Operation

        running_op, done_op, pending_op = mock.Mock(), mock.Mock(), mock.Mock()
        running_op.status = Operation.Status.RUNNING
        done_op.status = Operation.Status.DONE
        pending_op.status = Operation.Status.PENDING

        # Status goes from Running -> Pending -> Done
        operation_mock.side_effect = [pending_op, done_op]
        self.gke_hook._wait_for_operation(running_op)

        self.assertEqual(time_mock.call_count, 3)
        operation_mock.assert_any_call(running_op.name)
        operation_mock.assert_any_call(pending_op.name)
        self.assertEqual(operation_mock.call_count, 2)
