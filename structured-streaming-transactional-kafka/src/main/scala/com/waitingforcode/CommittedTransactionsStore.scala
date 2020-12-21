package com.waitingforcode

import org.mapdb.{DBMaker, Serializer}

/**
 * Simulates a distributed K/V store here.
 */
object CommittedTransactionsStore {

  private val committedTransactionsStoreBackend = DBMaker
    .fileDB("/tmp/waitingforcode/kafka-transactions-store")
    .fileMmapEnableIfSupported()
    .make()

  private val committedTransactionsStore = committedTransactionsStoreBackend
    .hashMap("committed-transactions", Serializer.LONG, Serializer.LONG)
    .createOrOpen()

  def commitTransaction(partitionId: Long, epochId: Long) = {
    committedTransactionsStore.put(partitionId, epochId)
    committedTransactionsStoreBackend.commit()
  }

  def getLastCommitForPartition(partitionId: Long) = {
    println(s"Getting last commit for ${partitionId}")
    committedTransactionsStore.get(partitionId)
  }

}
