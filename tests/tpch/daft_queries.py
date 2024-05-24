import pytest
import datetime

daft = pytest.importorskip("daft")


def query_1(dataset_path, scale):
    lineitem = daft.read_parquet(dataset_path + "lineitem")

    discounted_price = daft.col("l_extendedprice") * (1 - daft.col("l_discount"))
    taxed_discounted_price = discounted_price * (1 + daft.col("l_tax"))
    daft_df = (
        lineitem.where(daft.col("l_shipdate") <= datetime.date(1998, 9, 2))
        .groupby(daft.col("l_returnflag"), daft.col("l_linestatus"))
        .agg(
            daft.col("l_quantity").sum().alias("sum_qty"),
            daft.col("l_extendedprice").sum().alias("sum_base_price"),
            discounted_price.sum().alias("sum_disc_price"),
            taxed_discounted_price.sum().alias("sum_charge"),
            daft.col("l_quantity").mean().alias("avg_qty"),
            daft.col("l_extendedprice").mean().alias("avg_price"),
            daft.col("l_discount").mean().alias("avg_disc"),
            daft.col("l_quantity").count().alias("count_order"),
        )
        .sort(["l_returnflag", "l_linestatus"])
    )
    return daft_df.to_arrow()


def query_2(dataset_path, scale):
    region = daft.read_parquet(dataset_path + "region")
    supplier = daft.read_parquet(dataset_path + "supplier")
    partsupp = daft.read_parquet(dataset_path + "partsupp")
    nation = daft.read_parquet(dataset_path + "nation")
    part = daft.read_parquet(dataset_path + "part")

    europe = (
        region.where(daft.col("r_name") == "EUROPE")
        .join(nation, left_on=daft.col("r_regionkey"), right_on=daft.col("n_regionkey"))
        .join(supplier, left_on=daft.col("n_nationkey"), right_on=daft.col("s_nationkey"))
        .join(partsupp, left_on=daft.col("s_suppkey"), right_on=daft.col("ps_suppkey"))
    )

    brass = part.where((daft.col("p_size") == 15) & daft.col("p_type").str.endswith("BRASS")).join(
        europe,
        left_on=daft.col("p_partkey"),
        right_on=daft.col("ps_partkey"),
    )
    min_cost = brass.groupby(daft.col("p_partkey")).agg(daft.col("ps_supplycost").min().alias("min"))

    daft_df = (
        brass.join(min_cost, on=daft.col("p_partkey"))
        .where(daft.col("ps_supplycost") == daft.col("min"))
        .select(
            daft.col("s_acctbal"),
            daft.col("s_name"),
            daft.col("n_name"),
            daft.col("p_partkey"),
            daft.col("p_mfgr"),
            daft.col("s_address"),
            daft.col("s_phone"),
            daft.col("s_comment"),
        )
        .sort(by=["s_acctbal", "n_name", "s_name", "p_partkey"], desc=[True, False, False, False])
        .limit(100)
    )
    return daft_df.to_arrow()


def query_3(dataset_path, scale):
    def decrease(x, y):
        return x * (1 - y)
   
    lineitem = daft.read_parquet(dataset_path + "lineitem")
    orders = daft.read_parquet(dataset_path + "orders")
    customer = daft.read_parquet(dataset_path + "customer")   

    customer = customer.where(daft.col("c_mktsegment") == "BUILDING")
    orders = orders.where(daft.col("o_orderdate") < datetime.date(1995, 3, 15))
    lineitem = lineitem.where(daft.col("l_shipdate") > datetime.date(1995, 3, 15))

    daft_df = (
        customer.join(orders, left_on=daft.col("c_custkey"), right_on=daft.col("o_custkey"))
        .select(daft.col("o_orderkey"), daft.col("o_orderdate"), daft.col("o_shippriority"))
        .join(lineitem, left_on=daft.col("o_orderkey"), right_on=daft.col("l_orderkey"))
        .select(
            daft.col("o_orderkey"),
            decrease(daft.col("l_extendedprice"), daft.col("l_discount")).alias("volume"),
            daft.col("o_orderdate"),
            daft.col("o_shippriority"),
        )
        .groupby(daft.col("o_orderkey"), daft.col("o_orderdate"), daft.col("o_shippriority"))
        .agg(daft.col("volume").sum().alias("revenue"))
        .sort(by=["revenue", "o_orderdate"], desc=[True, False])
        .limit(10)
        .select("o_orderkey", "revenue", "o_orderdate", "o_shippriority")
    )
    return daft_df.to_arrow()


def query_4(dataset_path, scale):
    lineitem = daft.read_parquet(dataset_path + "lineitem")
    orders = daft.read_parquet(dataset_path + "orders")
    orders = orders.where(
        (daft.col("o_orderdate") >= datetime.date(1993, 7, 1)) & (daft.col("o_orderdate") < datetime.date(1993, 10, 1))
    )

    lineitem = lineitem.where(daft.col("l_commitdate") < daft.col("l_receiptdate")).select(daft.col("l_orderkey")).distinct()

    daft_df = (
        lineitem.join(orders, left_on=daft.col("l_orderkey"), right_on=daft.col("o_orderkey"))
        .groupby(daft.col("o_orderpriority"))
        .agg(daft.col("l_orderkey").count().alias("order_count"))
        .sort(daft.col("o_orderpriority"))
    )
    return daft_df.to_arrow()


def query_5(dataset_path, scale):
    lineitem = daft.read_parquet(dataset_path + "lineitem")
    supplier = daft.read_parquet(dataset_path + "supplier")
    nation = daft.read_parquet(dataset_path + "nation")
    region = daft.read_parquet(dataset_path + "region")
    orders = daft.read_parquet(dataset_path + "orders")
    customer = daft.read_parquet(dataset_path + "customer")
    orders = orders.where(
        (daft.col("o_orderdate") >= datetime.date(1994, 1, 1)) & (daft.col("o_orderdate") < datetime.date(1995, 1, 1))
    )
    region = region.where(daft.col("r_name") == "ASIA")

    # NOTE joins are manually re-ordered using Dask's manual ordering
    # because Daft does not yet have join re-ordering either
    daft_df = (
        region.join(nation, left_on=daft.col("r_regionkey"), right_on=daft.col("n_regionkey"))
        .join(customer, left_on=daft.col("n_nationkey"), right_on=daft.col("c_nationkey"))
        .join(orders, left_on=daft.col("c_custkey"), right_on=daft.col("o_custkey"))
        .join(lineitem, left_on=daft.col("o_orderkey"), right_on=daft.col("l_orderkey"))
        .join(supplier, left_on=[daft.col("l_suppkey"), daft.col("n_nationkey")], right_on=[daft.col("s_suppkey"), daft.col("s_nationkey")])
        .with_column("value", daft.col("l_extendedprice") * (1 - daft.col("l_discount")))
        .groupby(daft.col("n_name"))
        .agg(daft.col("value").sum().alias("revenue"))
        .sort(daft.col("revenue"), desc=True)
    )
    return daft_df.to_arrow()


def query_6(dataset_path, scale):
    lineitem = daft.read_parquet(dataset_path + "lineitem")
    daft_df = lineitem.where(
        (daft.col("l_shipdate") >= datetime.date(1994, 1, 1))
        & (daft.col("l_shipdate") < datetime.date(1995, 1, 1))
        & (daft.col("l_discount") >= 0.05)
        & (daft.col("l_discount") <= 0.07)
        & (daft.col("l_quantity") < 24)
    ).sum(daft.col("l_extendedprice") * daft.col("l_discount"))
    return daft_df.to_arrow()


def query_7(dataset_path, scale):
    def decrease(x, y):
        return x * (1 - y)

    lineitem = daft.read_parquet(dataset_path + "lineitem")
    supplier = daft.read_parquet(dataset_path + "supplier")
    nation = daft.read_parquet(dataset_path + "nation")
    orders = daft.read_parquet(dataset_path + "orders")
    customer = daft.read_parquet(dataset_path + "customer")

    lineitem = lineitem.where(
        (daft.col("l_shipdate") >= datetime.date(1995, 1, 1)) & (daft.col("l_shipdate") <= datetime.date(1996, 12, 31))
    )
    nation = nation.where((daft.col("n_name") == "FRANCE") | (daft.col("n_name") == "GERMANY"))

    supNation = (
        nation.join(supplier, left_on=daft.col("n_nationkey"), right_on=daft.col("s_nationkey"))
        .join(lineitem, left_on=daft.col("s_suppkey"), right_on=daft.col("l_suppkey"))
        .select(
            daft.col("n_name").alias("supp_nation"),
            daft.col("l_orderkey"),
            daft.col("l_extendedprice"),
            daft.col("l_discount"),
            daft.col("l_shipdate"),
        )
    )

    daft_df = (
        nation.join(customer, left_on=daft.col("n_nationkey"), right_on=daft.col("c_nationkey"))
        .join(orders, left_on=daft.col("c_custkey"), right_on=daft.col("o_custkey"))
        .select(daft.col("n_name").alias("cust_nation"), daft.col("o_orderkey"))
        .join(supNation, left_on=daft.col("o_orderkey"), right_on=daft.col("l_orderkey"))
        .where(
            ((daft.col("supp_nation") == "FRANCE") & (daft.col("cust_nation") == "GERMANY"))
            | ((daft.col("supp_nation") == "GERMANY") & (daft.col("cust_nation") == "FRANCE"))
        )
        .select(
            daft.col("supp_nation"),
            daft.col("cust_nation"),
            daft.col("l_shipdate").dt.year().alias("l_year"),
            decrease(daft.col("l_extendedprice"), daft.col("l_discount")).alias("volume"),
        )
        .groupby(daft.col("supp_nation"), daft.col("cust_nation"), daft.col("l_year"))
        .agg(daft.col("volume").sum().alias("revenue"))
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )
    return daft_df.to_arrow()


def query_8(dataset_path, scale):
    def decrease(x, y):
        return x * (1 - y)
    
    lineitem = daft.read_parquet(dataset_path + "lineitem")
    supplier = daft.read_parquet(dataset_path + "supplier")
    nation = daft.read_parquet(dataset_path + "nation")
    region = daft.read_parquet(dataset_path + "region")
    orders = daft.read_parquet(dataset_path + "orders")
    customer = daft.read_parquet(dataset_path + "customer")
    part = daft.read_parquet(dataset_path + "part")

    region = region.where(daft.col("r_name") == "AMERICA")
    orders = orders.where(
        (daft.col("o_orderdate") <= datetime.date(1996, 12, 31)) & (daft.col("o_orderdate") >= datetime.date(1995, 1, 1))
    )
    part = part.where(daft.col("p_type") == "ECONOMY ANODIZED STEEL")

    nat = nation.join(supplier, left_on=daft.col("n_nationkey"), right_on=daft.col("s_nationkey"))

    line = (
        lineitem.select(
            daft.col("l_partkey"),
            daft.col("l_suppkey"),
            daft.col("l_orderkey"),
            decrease(daft.col("l_extendedprice"), daft.col("l_discount")).alias("volume"),
        )
        .join(part, left_on=daft.col("l_partkey"), right_on=daft.col("p_partkey"))
        .join(nat, left_on=daft.col("l_suppkey"), right_on=daft.col("s_suppkey"))
    )

    daft_df = (
        nation.join(region, left_on=daft.col("n_regionkey"), right_on=daft.col("r_regionkey"))
        .select(daft.col("n_nationkey"))
        .join(customer, left_on=daft.col("n_nationkey"), right_on=daft.col("c_nationkey"))
        .select(daft.col("c_custkey"))
        .join(orders, left_on=daft.col("c_custkey"), right_on=daft.col("o_custkey"))
        .select(daft.col("o_orderkey"), daft.col("o_orderdate"))
        .join(line, left_on=daft.col("o_orderkey"), right_on=daft.col("l_orderkey"))
        .select(
            daft.col("o_orderdate").dt.year().alias("o_year"),
            daft.col("volume"),
            (daft.col("n_name") == "BRAZIL").if_else(daft.col("volume"), 0.0).alias("case_volume"),
        )
        .groupby(daft.col("o_year"))
        .agg(daft.col("case_volume").sum().alias("case_volume_sum"), daft.col("volume").sum().alias("volume_sum"))
        .select(daft.col("o_year"), daft.col("case_volume_sum") / daft.col("volume_sum"))
        .sort(daft.col("o_year"))
    )

    return daft_df.to_arrow()


def query_9(dataset_path, scale):
    def expr(x, y, v, w):
        return x * (1 - y) - (v * w)

    lineitem = daft.read_parquet(dataset_path + "lineitem")
    supplier = daft.read_parquet(dataset_path + "supplier")
    partsupp = daft.read_parquet(dataset_path + "partsupp")
    nation = daft.read_parquet(dataset_path + "nation")
    orders = daft.read_parquet(dataset_path + "orders")
    part = daft.read_parquet(dataset_path + "part")

    part = part.where(daft.col("p_name").str.contains("green"))

    # NOTE joins are manually re-ordered using Dask's manual ordering
    # because Daft does not yet have join re-ordering either
    daft_df = part.join(
        partsupp, left_on="p_partkey", right_on="ps_partkey"
    ).join(
        supplier, left_on="ps_suppkey", right_on="s_suppkey"
    ).join(
        lineitem,
        left_on=["ps_partkey", "ps_suppkey"],
        right_on=["l_partkey", "l_suppkey"],
    ).join(
        orders,
        left_on="l_orderkey",
        right_on="o_orderkey",
    ).join(
        nation,
        left_on="s_nationkey",
        right_on="n_nationkey",
    ).select(
        daft.col("n_name"),
        daft.col("o_orderdate").dt.year().alias("o_year"),
        expr(daft.col("l_extendedprice"), daft.col("l_discount"), daft.col("ps_supplycost"), daft.col("l_quantity")).alias("amount"),
    ).groupby(
        daft.col("n_name"),
        daft.col("o_year"),
    ).agg(
        daft.col("amount").sum()
    ).sort(
        by=["n_name", "o_year"],
        desc=[False, True]
    )
    return daft_df.to_arrow()


def query_10(dataset_path, scale):
    def decrease(x, y):
        return x * (1 - y)

    lineitem = daft.read_parquet(dataset_path + "lineitem")
    nation = daft.read_parquet(dataset_path + "nation")
    orders = daft.read_parquet(dataset_path + "orders")
    customer = daft.read_parquet(dataset_path + "customer")

    lineitem = lineitem.where(daft.col("l_returnflag") == "R")
    daft_df = (
        orders.where(
            (daft.col("o_orderdate") < datetime.date(1994, 1, 1)) & (daft.col("o_orderdate") >= datetime.date(1993, 10, 1))
        )
        .join(customer, left_on=daft.col("o_custkey"), right_on=daft.col("c_custkey"))
        .join(nation, left_on=daft.col("c_nationkey"), right_on=daft.col("n_nationkey"))
        .join(lineitem, left_on=daft.col("o_orderkey"), right_on=daft.col("l_orderkey"))
        .select(
            daft.col("o_custkey"),
            daft.col("c_name"),
            decrease(daft.col("l_extendedprice"), daft.col("l_discount")).alias("volume"),
            daft.col("c_acctbal"),
            daft.col("n_name"),
            daft.col("c_address"),
            daft.col("c_phone"),
            daft.col("c_comment"),
        )
        .groupby(
            daft.col("o_custkey"),
            daft.col("c_name"),
            daft.col("c_acctbal"),
            daft.col("c_phone"),
            daft.col("n_name"),
            daft.col("c_address"),
            daft.col("c_comment"),
        )
        .agg(daft.col("volume").sum().alias("revenue"))
        .sort(daft.col("revenue"), desc=True)
        .select(
            daft.col("o_custkey"),
            daft.col("c_name"),
            daft.col("revenue"),
            daft.col("c_acctbal"),
            daft.col("n_name"),
            daft.col("c_address"),
            daft.col("c_phone"),
            daft.col("c_comment"),
        )
        .limit(20)
    )

    return daft_df.to_arrow()


def query_11(dataset_path, scale):
    supplier = daft.read_parquet(dataset_path + "supplier")
    partsupp = daft.read_parquet(dataset_path + "partsupp")
    nation = daft.read_parquet(dataset_path + "nation")

    var_1 = "GERMANY"
    var_2 = 0.0001 / 1

    res_1 = (
        partsupp.join(supplier, left_on=daft.col("ps_suppkey"), right_on=daft.col("s_suppkey"))
        .join(nation, left_on=daft.col("s_nationkey"), right_on=daft.col("n_nationkey"))
        .where(daft.col("n_name") == var_1)
    )

    res_2 = res_1.agg((daft.col("ps_supplycost") * daft.col("ps_availqty")).sum().alias("tmp")).select(
        daft.col("tmp") * var_2, daft.lit(1).alias("lit")
    )

    daft_df = (
        res_1.groupby("ps_partkey")
        .agg(
            (daft.col("ps_supplycost") * daft.col("ps_availqty")).sum().alias("value"),
        )
        .with_column("lit", daft.lit(1))
        .join(res_2, on="lit")
        .where(daft.col("value") > daft.col("tmp"))
        .select(daft.col("ps_partkey"), daft.col("value").round(2))
        .sort(daft.col("value"), desc=True)
    )

    return daft_df.to_arrow()


def query_12(dataset_path, scale):
    orders = daft.read_parquet(dataset_path + "orders")
    lineitem = daft.read_parquet(dataset_path + "lineitem")

    daft_df = (
        orders.join(lineitem, left_on=daft.col("o_orderkey"), right_on=daft.col("l_orderkey"))
        .where(
            daft.col("l_shipmode").is_in(["MAIL", "SHIP"])
            & (daft.col("l_commitdate") < daft.col("l_receiptdate"))
            & (daft.col("l_shipdate") < daft.col("l_commitdate"))
            & (daft.col("l_receiptdate") >= datetime.date(1994, 1, 1))
            & (daft.col("l_receiptdate") < datetime.date(1995, 1, 1))
        )
        .groupby(daft.col("l_shipmode"))
        .agg(
            ((daft.col("o_orderpriority") == "1-URGENT") | (daft.col("o_orderpriority") == "2-HIGH"))
            .if_else(1, 0)
            .sum()
            .alias("high_line_count"),
            ((daft.col("o_orderpriority") != "1-URGENT") & (daft.col("o_orderpriority") != "2-HIGH"))
            .if_else(1, 0)
            .sum()
            .alias("low_line_count"),
        )
        .sort(daft.col("l_shipmode"))
    )

    return daft_df.to_arrow()


def query_13(dataset_path, scale):
    orders = daft.read_parquet(dataset_path + "orders")
    customer = daft.read_parquet(dataset_path + "customer")

    daft_df = (
        customer.join(
            orders.where(~daft.col("o_comment").str.match(".*special.*requests.*")),
            left_on="c_custkey",
            right_on="o_custkey",
            how="left",
        )
        .groupby(daft.col("c_custkey"))
        .agg(daft.col("o_orderkey").count().alias("c_count"))
        .sort("c_custkey")
        .groupby("c_count")
        .agg(daft.col("c_count").count().alias("custdist"))
        .sort(["custdist", "c_count"], desc=[True, True])
    )

    return daft_df.to_arrow()


def query_14(dataset_path, scale):
    part = daft.read_parquet(dataset_path + "part")
    lineitem = daft.read_parquet(dataset_path + "lineitem")

    daft_df = (
        lineitem.join(part, left_on=daft.col("l_partkey"), right_on=daft.col("p_partkey"))
        .where((daft.col("l_shipdate") >= datetime.date(1995, 9, 1)) & (daft.col("l_shipdate") < datetime.date(1995, 10, 1)))
        .agg(
            daft.col("p_type")
            .str.startswith("PROMO")
            .if_else(daft.col("l_extendedprice") * (1 - daft.col("l_discount")), 0)
            .sum()
            .alias("tmp_1"),
            (daft.col("l_extendedprice") * (1 - daft.col("l_discount"))).sum().alias("tmp_2"),
        )
        .select(100.00 * (daft.col("tmp_1") / daft.col("tmp_2")).alias("promo_revenue"))
    )

    return daft_df.to_arrow()


def query_15(dataset_path, scale):
    supplier = daft.read_parquet(dataset_path + "supplier")
    lineitem = daft.read_parquet(dataset_path + "lineitem")

    revenue = (
        lineitem.where(
            (daft.col("l_shipdate") >= datetime.date(1996, 1, 1)) & (daft.col("l_shipdate") < datetime.date(1996, 4, 1))
        )
        .groupby(daft.col("l_suppkey"))
        .agg((daft.col("l_extendedprice") * (1 - daft.col("l_discount"))).sum().alias("total_revenue"))
        .select(daft.col("l_suppkey").alias("supplier_no"), "total_revenue")
    )

    revenue = revenue.join(revenue.max("total_revenue"), on="total_revenue")

    daft_df = (
        supplier.join(revenue, left_on=daft.col("s_suppkey"), right_on=daft.col("supplier_no"))
        .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
        .sort("s_suppkey")
    )

    return daft_df.to_arrow()


def query_16(dataset_path, scale):
    supplier = daft.read_parquet(dataset_path + "supplier")
    partsupp = daft.read_parquet(dataset_path + "partsupp")
    part = daft.read_parquet(dataset_path + "part")

    suppkeys = supplier.where(daft.col("s_comment").str.match(".*Customer.*Complaints.*")).select(
        daft.col("s_suppkey"), daft.col("s_suppkey").alias("ps_suppkey_right")
    )

    daft_df = (
        part.join(partsupp, left_on=daft.col("p_partkey"), right_on=daft.col("ps_partkey"))
        .where(
            (daft.col("p_brand") != "Brand#45")
            & ~daft.col("p_type").str.startswith("MEDIUM POLISHED")
            & (daft.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        )
        .join(suppkeys, left_on="ps_suppkey", right_on="s_suppkey", how="left")
        .where(daft.col("ps_suppkey_right").is_null())
        .select("p_brand", "p_type", "p_size", "ps_suppkey")
        .distinct()
        .groupby("p_brand", "p_type", "p_size")
        .agg(daft.col("ps_suppkey").count().alias("supplier_cnt"))
        .sort(["supplier_cnt", "p_brand", "p_type", "p_size"], desc=[True, False, False, False])
    )

    return daft_df.to_arrow()


def query_17(dataset_path, scale):
    part = daft.read_parquet(dataset_path + "part")
    lineitem = daft.read_parquet(dataset_path + "lineitem")

    res_1 = part.where((daft.col("p_brand") == "Brand#23") & (daft.col("p_container") == "MED BOX")).join(
        lineitem, left_on="p_partkey", right_on="l_partkey", how="left"
    )

    daft_df = (
        res_1.groupby("p_partkey")
        .agg((0.2 * daft.col("l_quantity")).mean().alias("avg_quantity"))
        .select(daft.col("p_partkey").alias("key"), daft.col("avg_quantity"))
        .join(res_1, left_on="key", right_on="p_partkey")
        .where(daft.col("l_quantity") < daft.col("avg_quantity"))
        .agg((daft.col("l_extendedprice") / 7.0).sum().alias("avg_yearly"))
    )

    return daft_df.to_arrow()


def query_18(dataset_path, scale):
    orders = daft.read_parquet(dataset_path + "orders")
    lineitem = daft.read_parquet(dataset_path + "lineitem")
    customer = daft.read_parquet(dataset_path + "customer")

    res_1 = lineitem.groupby("l_orderkey").agg(daft.col("l_quantity").sum().alias("sum_qty")).where(daft.col("sum_qty") > 300)

    daft_df = (
        orders.join(res_1, left_on=daft.col("o_orderkey"), right_on=daft.col("l_orderkey"))
        .join(customer, left_on=daft.col("o_custkey"), right_on=daft.col("c_custkey"))
        .join(lineitem, left_on=daft.col("o_orderkey"), right_on=daft.col("l_orderkey"))
        .groupby("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
        .agg(daft.col("l_quantity").sum().alias("sum"))
        .select("c_name", "c_custkey", "o_orderkey", daft.col("o_orderdate").alias("o_orderdat"), "o_totalprice", "sum")
        .sort(["o_totalprice", "o_orderdat"], desc=[True, False])
        .limit(100)
    )

    return daft_df.to_arrow()


def query_19(dataset_path, scale):
    part = daft.read_parquet(dataset_path + "part")
    lineitem = daft.read_parquet(dataset_path + "lineitem")

    daft_df = (
        lineitem.join(part, left_on=daft.col("l_partkey"), right_on=daft.col("p_partkey"))
        .where(
            (
                (daft.col("p_brand") == "Brand#12")
                & daft.col("p_container").is_in(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])
                & (daft.col("l_quantity") >= 1)
                & (daft.col("l_quantity") <= 11)
                & (daft.col("p_size") >= 1)
                & (daft.col("p_size") <= 5)
                & daft.col("l_shipmode").is_in(["AIR", "AIR REG"])
                & (daft.col("l_shipinstruct") == "DELIVER IN PERSON")
            )
            | (
                (daft.col("p_brand") == "Brand#23")
                & daft.col("p_container").is_in(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])
                & (daft.col("l_quantity") >= 10)
                & (daft.col("l_quantity") <= 20)
                & (daft.col("p_size") >= 1)
                & (daft.col("p_size") <= 10)
                & daft.col("l_shipmode").is_in(["AIR", "AIR REG"])
                & (daft.col("l_shipinstruct") == "DELIVER IN PERSON")
            )
            | (
                (daft.col("p_brand") == "Brand#34")
                & daft.col("p_container").is_in(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])
                & (daft.col("l_quantity") >= 20)
                & (daft.col("l_quantity") <= 30)
                & (daft.col("p_size") >= 1)
                & (daft.col("p_size") <= 15)
                & daft.col("l_shipmode").is_in(["AIR", "AIR REG"])
                & (daft.col("l_shipinstruct") == "DELIVER IN PERSON")
            )
        )
        .agg((daft.col("l_extendedprice") * (1 - daft.col("l_discount"))).sum().alias("revenue"))
    )

    return daft_df.to_arrow()


def query_20(dataset_path, scale):
    supplier = daft.read_parquet(dataset_path + "supplier")
    partsupp = daft.read_parquet(dataset_path + "partsupp")
    nation = daft.read_parquet(dataset_path + "nation")
    part = daft.read_parquet(dataset_path + "part")
    lineitem = daft.read_parquet(dataset_path + "lineitem")

    res_1 = (
        lineitem.where(
            (daft.col("l_shipdate") >= datetime.date(1994, 1, 1)) & (daft.col("l_shipdate") < datetime.date(1995, 1, 1))
        )
        .groupby("l_partkey", "l_suppkey")
        .agg(((daft.col("l_quantity") * 0.5).sum()).alias("sum_quantity"))
    )

    res_2 = nation.where(daft.col("n_name") == "CANADA")
    res_3 = supplier.join(res_2, left_on="s_nationkey", right_on="n_nationkey")

    daft_df = (
        part.where(daft.col("p_name").str.startswith("forest"))
        .select("p_partkey")
        .distinct()
        .join(partsupp, left_on="p_partkey", right_on="ps_partkey")
        .join(
            res_1,
            left_on=["ps_suppkey", "p_partkey"],
            right_on=["l_suppkey", "l_partkey"],
        )
        .where(daft.col("ps_availqty") > daft.col("sum_quantity"))
        .select("ps_suppkey")
        .distinct()
        .join(res_3, left_on="ps_suppkey", right_on="s_suppkey")
        .select("s_name", "s_address")
        .sort("s_name")
    )

    return daft_df.to_arrow()


def query_21(dataset_path, scale):
    supplier = daft.read_parquet(dataset_path + "supplier")
    nation = daft.read_parquet(dataset_path + "nation")
    orders = daft.read_parquet(dataset_path + "orders")
    lineitem = daft.read_parquet(dataset_path + "lineitem")

    res_1 = (
        lineitem.select("l_suppkey", "l_orderkey")
        .distinct()
        .groupby("l_orderkey")
        .agg(daft.col("l_suppkey").count().alias("nunique_col"))
        .where(daft.col("nunique_col") > 1)
        .join(lineitem.where(daft.col("l_receiptdate") > daft.col("l_commitdate")), on="l_orderkey")
    )

    daft_df = (
        res_1.select("l_suppkey", "l_orderkey")
        .distinct()
        .groupby("l_orderkey")
        .agg(daft.col("l_suppkey").count().alias("nunique_col"))
        .join(res_1, on="l_orderkey")
        .join(orders, left_on="l_orderkey", right_on="o_orderkey")
        .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
        .join(nation, left_on="s_nationkey", right_on="n_nationkey")
        .where((daft.col("nunique_col") == 1) & (daft.col("n_name") == "SAUDI ARABIA") & (daft.col("o_orderstatus") == "F"))
        .groupby("s_name")
        .agg(daft.col("o_orderkey").count().alias("numwait"))
        .sort(["numwait", "s_name"], desc=[True, False])
        .limit(100)
    )

    return daft_df.to_arrow()


def query_22(dataset_path, scale):
    orders = daft.read_parquet(dataset_path + "orders")
    customer = daft.read_parquet(dataset_path + "customer")

    res_1 = (
        customer.with_column("cntrycode", daft.col("c_phone").str.left(2))
        .where(daft.col("cntrycode").is_in(["13", "31", "23", "29", "30", "18", "17"]))
        .select("c_acctbal", "c_custkey", "cntrycode")
    )

    res_2 = (
        res_1.where(daft.col("c_acctbal") > 0).agg(daft.col("c_acctbal").mean().alias("avg_acctbal")).with_column("lit", daft.lit(1))
    )

    res_3 = orders.select("o_custkey").distinct()

    daft_df = (
        res_1.join(res_3, left_on="c_custkey", right_on="o_custkey", how="left")
        .where(daft.col("o_custkey").is_null())
        .with_column("lit", daft.lit(1))
        .join(res_2, on="lit")
        .where(daft.col("c_acctbal") > daft.col("avg_acctbal"))
        .groupby("cntrycode")
        .agg(
            daft.col("c_acctbal").count().alias("numcust"),
            daft.col("c_acctbal").sum().alias("totacctbal"),
        )
        .sort("cntrycode")
    )

    return daft_df.to_arrow()
