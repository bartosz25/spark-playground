package com.waitingforcode

object ExceptDemo extends App {

  import sparkSession.implicits._

  val df1 = ((1 to 10).map(nr => (nr, s"Number ${nr}")) ++ (1 to 10).map(nr => (nr, s"Number ${nr}"))).toDF("nr", "label")
  val df2 = (3 to 13).map(nr => (nr, s"Number ${nr}")).toDF("nr", "label")

  // Equality checking is performed directly on the encoded representation of the data and thus is not affected by a custom equals function defined on T.
  val missingRowsInDf1 = df1.except(df2)
  missingRowsInDf1.explain(extended = true)
  missingRowsInDf1.show(truncate = false)

  val missingRowsInDf1D = df1.exceptAll(df2)
  missingRowsInDf1D.explain(extended = true)
  missingRowsInDf1D.show()
  // Here we have: rows 1 and 2 from the df1 not in df2
  //               rows 1-10 duplicated from df1 not in df2

  /*
  PySpark's subtract is an alias for except:
      def subtract(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows in this :class:`DataFrame`
        but not in another :class:`DataFrame`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be subtracted.

        Returns
        -------
        :class:`DataFrame`
            Subtracted DataFrame.

        Notes
        -----
        This is equivalent to `EXCEPT DISTINCT` in SQL.

        Examples
        --------
        >>> df1 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3)], ["C1", "C2"])
        >>> df1.subtract(df2).show()
        +---+---+
        | C1| C2|
        +---+---+
        |  c|  4|
        +---+---+
        """
        return DataFrame(getattr(self._jdf, "except")(other._jdf), self.sparkSession)

   */
}
