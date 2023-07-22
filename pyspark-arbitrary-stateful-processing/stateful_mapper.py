import datetime
from typing import Any, Dict, Iterable

import pandas
from pyspark.sql.streaming.state import GroupState


def map_with_state(group_id_tuple: Any,
                   input_rows: Iterable[pandas.DataFrame],
                   current_state: GroupState) -> Iterable[pandas.DataFrame]:
    session_expiration_time_16min_as_ms = 16 * 60 * 1000
    group_id = group_id_tuple[0]

    def generate_final_output(start_time_for_output: datetime.datetime, end_time_for_output: datetime.datetime,
                              is_final: bool) -> Dict[str, Any]:
        duration_in_milliseconds = (end_time_for_output - start_time_for_output).total_seconds() * 1000
        return {
            "group_id": [group_id],
            "start_time": [start_time_for_output.isoformat()],
            "end_time": [end_time_for_output.isoformat()],
            "duration_in_milliseconds": [int(duration_in_milliseconds)],
            "is_final": [is_final]
        }

    if current_state.hasTimedOut:
        print(f"Session ({current_state.get}) expired for {group_id}; let's generate the final output here")
        start_time, end_time,  = current_state.get
        record = generate_final_output(start_time, end_time, is_final=True)
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
        if current_state.exists:
            start_time, end_time, = current_state.get
            start_time_for_sink = min(start_time, first_event_timestamp_from_input)
            end_time_for_sink = max(end_time, last_event_timestamp_from_input)
            current_state.update((
                start_time_for_sink,
                end_time_for_sink
            ))
        else:
            current_state.update((first_event_timestamp_from_input, last_event_timestamp_from_input))

        timeout_timestamp = base_watermark + session_expiration_time_16min_as_ms
        current_state.setTimeoutTimestamp(timeout_timestamp)

        record = generate_final_output(start_time_for_sink, end_time_for_sink, is_final=False)

    yield pandas.DataFrame(record)
