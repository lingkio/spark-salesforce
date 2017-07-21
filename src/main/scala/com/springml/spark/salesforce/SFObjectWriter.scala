package com.springml.spark.salesforce

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import com.springml.salesforce.wave.api.APIFactory
import com.springml.salesforce.wave.api.BulkAPI
import com.springml.salesforce.wave.util.WaveAPIConstants

/**
 * Write class responsible for update Salesforce object using data provided in dataframe
 * First column of dataframe contains Salesforce Object
 * Next subsequent columns are fields to be updated
 */
class SFObjectWriter (
    val username: String,
    val password: String,
    val useSessionId: Boolean,
    val sessionId: String,
    val login: String,
    val apiVersion: String,
    val sfObject: String,
    val mode: SaveMode,
    val operation: String,
    val csvHeader: String,
    val concurrencyMode: String
    ) extends Serializable {

  @transient val logger = Logger.getLogger(classOf[SFObjectWriter])

  def writeData(rdd: RDD[Row]): Boolean = {
    // need to partition in such a way that each partition contains less than 10k records....  This is tricky to do without doing a count
    // of everything.
    // may be better to find an approach where new bulk job is created if we're going to pass 10000 records - can do by
    // getting slice of iterator during mapPartitionsWithIndex below.
    val rddCount = rdd.count

    val newNumPartitions = (rddCount / 10000).toInt + 1
    var csvRDD = rdd.map(row => row.toSeq.map(value => Utils.rowValue(value)).mkString(","))
    val numPartitions = rdd.getNumPartitions
    if (newNumPartitions > numPartitions) {
      // need more partitions so each batch is smaller than 10000
      csvRDD = csvRDD.repartition(newNumPartitions)
    }
    val opMode = operation(mode, operation)
    val jobId = APIFactory.getInstance.bulkAPI(username, password, useSessionId, sessionId, login, apiVersion, concurrencyMode).createJob(sfObject, opMode, WaveAPIConstants.STR_CSV).getId
    
    val success = csvRDD.mapPartitionsWithIndex {
      case (index, iterator) => {
        val records = iterator.toArray.mkString("\n")
        var batchInfoId : String = null
        if (records != null && !records.isEmpty()) {
          val data = csvHeader + "\n" + records
          val batchInfo = bulkAPI.addBatch(jobId, data)
          batchInfoId = batchInfo.getId
        }
        val success = (batchInfoId != null)
        if (success) {
            var i = 1
            while (i < 99999 && !bulkAPI.isCompleted(jobId)) {
                Thread.sleep(100)
                i = i + 1
            }
        }
        List(success).iterator
      }
    }.reduce((a, b) => a & b)

    bulkAPI.closeJob(jobId)
    true
  }

  def bulkAPI() : BulkAPI = {
    APIFactory.getInstance.bulkAPI(username, password, useSessionId, sessionId, login, apiVersion)
  }

  private def operation(mode: SaveMode, op: String): String = {
    if (mode != null && SaveMode.Overwrite.name().equalsIgnoreCase(mode.name())) {
      WaveAPIConstants.STR_UPDATE
    } else if (mode != null && SaveMode.Append.name().equalsIgnoreCase(mode.name())) {
      WaveAPIConstants.STR_INSERT
    } else if (op.equalsIgnoreCase("delete")) {
      "delete"
    } else {
      logger.warn("SaveMode " + mode + " Not supported. Using 'insert' operation")
      WaveAPIConstants.STR_INSERT
    }
  }

}