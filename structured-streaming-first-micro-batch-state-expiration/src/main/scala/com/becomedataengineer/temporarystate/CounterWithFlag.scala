package com.becomedataengineer.temporarystate

import java.sql.Timestamp

/**
 * @param firstGroupMaxEventTime Optionally stores the MAX(event_time) seen while creating the given
 *                               state. It's only presented for the second reading of the state group
 *                               for each state key.
 */
case class CounterWithFlag(count: Int, needsUpdateWithNewWatermark: Boolean = false,
                           firstGroupMaxEventTime: Option[Timestamp] = None)
