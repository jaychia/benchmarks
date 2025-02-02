import os

import coiled
import dask
import pandas as pd
import pytest
from distributed import LocalCluster

from .utils import get_answers_path, get_cluster_spec, get_dataset_path

pytestmark = pytest.mark.tpch_correctness


@pytest.fixture(params=[1, 10, 100], scope="session")
def scale(request):
    scale = request.param
    if scale != 100:
        pytest.skip(reason="Don't test everything by default")
    return scale


# Override identical fixture in conftest.py to use different scale
@pytest.fixture(scope="session")
def dataset_path(local, scale):
    return get_dataset_path(local, scale)


@pytest.fixture(scope="session")
def answers_path(local, scale):
    return get_answers_path(local, scale)


# Override identical fixture in conftest.py to use different scale
@pytest.fixture(scope="session")
def cluster_spec(scale, shutdown_on_close):
    return get_cluster_spec(scale=scale, shutdown_on_close=shutdown_on_close)


@pytest.fixture(scope="module")
def cluster(
    local,
    scale,
    module,
    dask_env_variables,
    cluster_spec,
    github_cluster_tags,
    name,
    make_chart,
):
    if local:
        with LocalCluster() as cluster:
            yield cluster
    else:
        kwargs = dict(
            name=f"tpch-{module}-{scale}-{name}",
            environ=dask_env_variables,
            tags=github_cluster_tags,
            region="us-east-2",
            **cluster_spec,
        )
        with dask.config.set({"distributed.scheduler.worker-saturation": "inf"}):
            with coiled.Cluster(**kwargs) as cluster:
                yield cluster


@pytest.fixture
def client(
    request,
    cluster,
    cluster_kwargs,
    get_cluster_info,
    performance_report,
    benchmark_time,
    span,
    restart,
    local,
    query,
    scale,
):
    with cluster.get_client() as client:
        if restart:
            client.restart()
        client.run(lambda: None)

        with get_cluster_info(cluster), performance_report, benchmark_time:
            yield client


def get_expected_answer(query: int, answers_path: str, s3_storage_options):
    answer = pd.read_parquet(
        os.path.join(answers_path, f"answer_{query}.parquet"),
        storage_options=s3_storage_options,
    )
    answer = answer.rename(columns=lambda x: x.strip())
    if "o_orderdate" in answer.columns:
        answer["o_orderdate"] = pd.to_datetime(answer["o_orderdate"])
    if "cntrycode" in answer.columns:
        answer["cntrycode"] = answer["cntrycode"].astype(str)

    return answer


def verify_result(
    result: pd.DataFrame, query: int, answers_path: str, s3_storage_options
):
    expected = get_expected_answer(query, answers_path, s3_storage_options)

    for column, dtype in expected.dtypes.items():
        if pd.api.types.is_object_dtype(dtype):
            result[column] = result[column].astype("str")
            expected[column] = expected[column].astype("str")
            # Some DuckDB results appear to be stripped, so strip them all for better comparison.
            result[column] = result[column].str.strip()
            expected[column] = expected[column].str.strip()

    # Query 11 is not deterministically sorted, there may be several 'ps_partkey' with the same,,l\\ 'value'
    if query == 11:
        assert result["value"].is_monotonic_decreasing
        assert expected["value"].is_monotonic_decreasing
        result = result.sort_values(["value", "ps_partkey"], ascending=[False, True])
        expected = expected.sort_values(
            ["value", "ps_partkey"], ascending=[False, True]
        )

    result = result.reset_index(drop=True)
    expected = expected.reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected, check_dtype=False, atol=1e-2)


@pytest.mark.tpch_correctness
@pytest.mark.parametrize(
    "query",
    [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
    ],
)
def test_dask_results(
    query, scale, local, dataset_path, answers_path, s3_storage_options, client
):
    from . import dask_queries

    func = getattr(dask_queries, f"query_{query}")
    result = func(dataset_path, None, scale).compute()
    verify_result(result, query, answers_path, s3_storage_options)
