import pytest
import botocore.session

pytestmark = pytest.mark.tpch_nondask

daft = pytest.importorskip("daft")

from . import daft_queries  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def setup(local, daft_ray_address):
    # Register s3 credentials with Daft if running in the cloud
    if not local:
        session = botocore.session.Session()
        creds = session.get_credentials()
        daft.set_planning_config(
            default_io_config=daft.io.IOConfig(
                s3=daft.io.S3Config(
                    region_name="us-east-2",
                    key_id=creds.access_key,
                    access_key=creds.secret_key,
                    session_token=creds.token,
                ),
            ),
        )

    # Connect to a Ray cluster if provided
    if daft_ray_address is not None:
        daft.context.set_runner_ray(address=daft_ray_address)

    return None


def test_query_1(run, dataset_path, scale):
    def _():
        daft_queries.query_1(dataset_path, scale)

    run(_)


def test_query_2(run, dataset_path, scale):
    def _():
        daft_queries.query_2(dataset_path, scale)

    run(_)


def test_query_3(run, dataset_path, scale):
    def _():
        daft_queries.query_3(dataset_path, scale)

    run(_)


def test_query_4(run, dataset_path, scale):
    def _():
        daft_queries.query_4(dataset_path, scale)

    run(_)


def test_query_5(run, dataset_path, scale):
    def _():
        daft_queries.query_5(dataset_path, scale)

    run(_)


def test_query_6(run, dataset_path, scale):
    def _():
        daft_queries.query_6(dataset_path, scale)

    run(_)


def test_query_7(run, dataset_path, scale):
    def _():
        daft_queries.query_7(dataset_path, scale)

    run(_)


def test_query_8(run, dataset_path, scale):
    def _():
        daft_queries.query_8(dataset_path, scale)

    run(_)


def test_query_9(run, dataset_path, scale):
    def _():
        daft_queries.query_9(dataset_path, scale)

    run(_)


def test_query_10(run, dataset_path, scale):
    def _():
        daft_queries.query_10(dataset_path, scale)

    run(_)


def test_query_11(run, dataset_path, scale):
    def _():
        daft_queries.query_11(dataset_path, scale)

    run(_)


def test_query_12(run, dataset_path, scale):
    def _():
        daft_queries.query_12(dataset_path, scale)

    run(_)


def test_query_13(run, dataset_path, scale):
    def _():
        daft_queries.query_13(dataset_path, scale)

    run(_)


def test_query_14(run, dataset_path, scale):
    def _():
        daft_queries.query_14(dataset_path, scale)

    run(_)


def test_query_15(run, dataset_path, scale):
    def _():
        daft_queries.query_15(dataset_path, scale)

    run(_)


def test_query_16(run, dataset_path, scale):
    def _():
        daft_queries.query_16(dataset_path, scale)

    run(_)


def test_query_17(run, dataset_path, scale):
    def _():
        daft_queries.query_17(dataset_path, scale)

    run(_)


def test_query_18(run, dataset_path, scale):
    def _():
        daft_queries.query_18(dataset_path, scale)

    run(_)


def test_query_19(run, dataset_path, scale):
    def _():
        daft_queries.query_19(dataset_path, scale)

    run(_)


def test_query_20(run, dataset_path, scale):
    def _():
        daft_queries.query_20(dataset_path, scale)

    run(_)


def test_query_21(run, dataset_path, scale):
    def _():
        daft_queries.query_21(dataset_path, scale)

    run(_)


def test_query_22(run, dataset_path, scale):
    def _():
        daft_queries.query_22(dataset_path, scale)

    run(_)
