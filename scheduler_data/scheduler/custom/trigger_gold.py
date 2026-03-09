from mage_ai.orchestration.triggers.api import trigger_pipeline

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


@custom
def trigger_next_pipeline(*args, **kwargs):
    trigger_pipeline(
        pipeline_uuid='dbt_build_gold',
    )