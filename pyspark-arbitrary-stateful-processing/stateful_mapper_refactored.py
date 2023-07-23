from typing import Any, Iterable

import pandas
from pyspark.sql.streaming.state import GroupState

from output_handlers import OutputHandler
from state_handlers import StateSchemaHandler


def map_with_state_refactored(state_handler: StateSchemaHandler, output_handler: OutputHandler,
                              group_id_tuple: Any,
                              input_rows: Iterable[pandas.DataFrame],
                              current_state: GroupState) -> Iterable[pandas.DataFrame]:
    group_id = group_id_tuple[0]

    if current_state.hasTimedOut:
        print(f"Session ({current_state.get}) expired for {group_id}; let's generate the final output here")
        current_state_as_dict = state_handler.get_state_as_dict(current_state.get)
        current_state.remove()
    else:
        should_use_event_time_for_watermark = current_state.getCurrentWatermarkMs() == 0
        base_watermark = current_state.getCurrentWatermarkMs()

        first_event_timestamp_from_input = None
        last_event_timestamp_from_input = None
        for input_df_for_group in input_rows:
            if should_use_event_time_for_watermark:
                input_df_for_group['event_time_as_milliseconds'] = input_df_for_group['timestamp'] \
                    .apply(lambda x: int(pandas.Timestamp(x).timestamp()) * 1000)
                base_watermark = int(input_df_for_group['event_time_as_milliseconds'].max())

            first_event_timestamp_from_input = input_df_for_group['timestamp'].min()
            last_event_timestamp_from_input = input_df_for_group['timestamp'].max()

        start_time_for_sink = first_event_timestamp_from_input
        end_time_for_sink = last_event_timestamp_from_input
        current_state_as_dict = state_handler.get_empty_state_dict()
        if current_state.exists:
            current_state_as_dict = state_handler.get_state_as_dict(current_state.get)
            start_time_for_sink = min(state_handler.start_time.get(current_state_as_dict),
                                      first_event_timestamp_from_input)
            end_time_for_sink = max(state_handler.start_time.get(current_state_as_dict),
                                    last_event_timestamp_from_input)

        state_handler.start_time.update(current_state_as_dict, start_time_for_sink)
        state_handler.end_time.update(current_state_as_dict, end_time_for_sink)

        state_to_update = StateSchemaHandler.transform_in_flight_state_to_state_to_write(current_state_as_dict)
        current_state.update(state_to_update)

        session_expiration_time_16min_as_ms = 16 * 60 * 1000
        timeout_timestamp = base_watermark + session_expiration_time_16min_as_ms
        current_state.setTimeoutTimestamp(timeout_timestamp)

    record = output_handler.generate_output(group_id=group_id,
                                            timed_out_state=current_state.hasTimedOut,
                                            state_dict=current_state_as_dict)
    yield pandas.DataFrame(record)
