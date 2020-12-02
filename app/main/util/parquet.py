def iter_parquet(tbl):
    # for group_i in range(tbl.num_row_groups):
    #     row_group = tbl.read_row_group(group_i)

    for batch in tbl.to_batches():
        for row in zip(*batch.columns):
            yield dict(zip(batch.schema.names, row))