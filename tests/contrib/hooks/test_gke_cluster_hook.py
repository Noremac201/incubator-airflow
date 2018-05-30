import unittest

from airflow.contrib.hooks.gke_cluster_hook import \
    GoogleKubernetesEngineClusterHook

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
        self.gke_hook = GoogleKubernetesEngineClusterHook()

    @mock.patch(
        "airflow.contrib.hooks.gke_cluster_hook.GoogleKubernetesEngineClusterHook._authorize")
    @mock.patch("googleapiclient.discovery.build")
    def test_get_conn(self, discovery_mock, authorize_mock):
        authorize_mock.return_value = 'http_object'
        self.gke_hook.get_conn()
        discovery_mock.assert_called_with('container', 'v1', http='http_object')

    @mock.patch(
        "airflow.contrib.hooks.gke_cluster_hook.GoogleKubernetesEngineClusterHook.get_conn")
    @mock.patch(
        "airflow.contrib.hooks.gke_cluster_hook.GoogleKubernetesEngineClusterHook._wait_for_response")
    def test_delete_cluster(self, wait_mock, conn_mock):
        self.gke_hook.delete_cluster(TEST_PROJECT_ID, CLUSTER_NAME, ZONE)

        response = conn_mock.return_value.projects.return_value.zones.return_value.clusters.return_value.delete.return_value

        conn_mock.assert_has_calls([
            mock.call().projects().zones().clusters().delete(
                projectId=TEST_PROJECT_ID, zone=ZONE,
                clusterId=CLUSTER_NAME)])

        wait_mock.assert_called_with(conn_mock(),
                                     response.execute().__getitem__())

    @mock.patch("time.sleep")
    def test_wait_for_response_succeed(self, time_mock):
        operation_name = 'test_op'

        mock_service = mock.Mock()

        response = mock_service.projects.return_value.zones.return_value.operations.return_value.get.return_value
        response.execute.return_value = {'done': True}
        expected_call = mock.call.projects().zones().operations().get(
            name=operation_name).execute().mock.call_list()

        self.gke_hook._wait_for_response(mock_service, operation_name)
        self.assertEqual(mock_service.mock_calls, expected_call)
        time_mock.assert_called_once()

    @mock.patch("time.sleep")
    def test_wait_for_response_fail_once(self, time_mock):
        operation_name = 'test_op'

        mock_service = mock.Mock()

        response = mock_service.projects.return_value.zones.return_value.operations.return_value.get.return_value
        response.execute.side_effect = [{'done': False}, {'done': True}]

        self.gke_hook._wait_for_response(mock_service, operation_name)
        self.assertIn(
            mock.call.projects().zones().operations().get(name=operation_name),
            mock_service.mock_calls)
        self.assertEqual(time_mock.call_count, 2)

    @mock.patch("time.sleep")
    def test_wait_for_response_error(self, time_mock):
        operation_name = 'test_op'

        mock_service = mock.Mock()

        response = mock_service.projects.return_value.zones.return_value.operations.return_value.get.return_value
        response.execute.return_value = {'done': True, 'error': True}

        with self.assertRaises(Exception) as context:
            self.gke_hook._wait_for_response(mock_service, operation_name)
            self.assertIn(mock.call.projects().zones().operations().get(
                name=operation_name), mock_service.mock_calls)
            self.assertEqual(time_mock.call_count, 1)
