"""Validate fixture facts with a Great Expectations runtime dataframe batch."""

import great_expectations as gx
import pandas as pd


def validate(facts):
    columns = ["tenant", "site", "dataset", "event_id", "product", "day", "quantity", "currency"]
    data = pd.DataFrame(facts, columns=columns)
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("silver")
    batch = source.add_dataframe_asset("sales").add_batch_definition_whole_dataframe("current").get_batch(
        batch_parameters={"dataframe": data},
    )
    exp = gx.expectations
    expectations = [
        *(exp.ExpectColumnValuesToNotBeNull(column=name) for name in columns),
        exp.ExpectCompoundColumnsToBeUnique(column_list=["dataset", "tenant", "site", "event_id"]),
        exp.ExpectColumnValuesToBeBetween(column="quantity", min_value=0, max_value=999999999),
        exp.ExpectColumnValuesToBeInSet(column="currency", value_set=["USD", "MAD", "GBP", "EUR"]),
    ]
    for expectation in expectations:
        if not batch.validate(expectation, result_format="BOOLEAN_ONLY").success:
            raise ValueError(f"silver_quality_gate:{type(expectation).__name__}")
    return len(expectations)
