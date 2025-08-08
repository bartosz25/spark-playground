package com

import java.sql.Timestamp
import java.time.Instant

package object waitingforcode {

  implicit def toTimestamp(time: String): Timestamp = {
    Timestamp.from(Instant.parse(s"2025-06-05T${time}.000Z"))
  }

}
