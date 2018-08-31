package com.springml.spark.salesforce

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.storage.StorageLevel
import com.springml.salesforce.wave.api.ForceAPI
import com.springml.salesforce.wave.api.WaveAPI
import com.springml.salesforce.wave.model.SOQLResult
import java.net.URLEncoder

/**
 * Relation class for reading data from Salesforce and construct RDD
 */
case class DatasetRelation(
    waveAPI: WaveAPI,
    forceAPI: ForceAPI,
    query: String,
    userSchema: StructType,
    sqlContext: SQLContext,
    resultVariable: Option[String],
    pageSize: Int,
    encodeFields: Option[String],
    inferSchema: Boolean) extends BaseRelation with TableScan {

  private val logger = Logger.getLogger(classOf[DatasetRelation])

  var resultSet: SOQLResult = null;
  var cachedResult: java.util.List[java.util.Map[String, String]] = null;

  private def readNext(): java.util.List[java.util.Map[String, String]] = {
    if (resultSet == null) {
	  resultSet = forceAPI.query(query)
	} else {
	  // we've already run a query, so need to read the next page
	  resultSet = forceAPI.queryMore(resultSet)
	}
    resultSet.filterRecords()
  }

  private def cast(fieldValue: String, toType: DataType,
      nullable: Boolean = true, fieldName: String): Any = {
    if (fieldValue == "" && nullable && !toType.isInstanceOf[StringType]) {
      null
    } else {
      toType match {
        case _: ByteType => fieldValue.toByte
        case _: ShortType => fieldValue.toShort
        case _: IntegerType => fieldValue.toInt
        case _: LongType => fieldValue.toLong
        case _: FloatType => fieldValue.toFloat
        case _: DoubleType => fieldValue.toDouble
        case _: BooleanType => fieldValue.toBoolean
        case _: DecimalType => new BigDecimal(fieldValue.replaceAll(",", ""))
        case _: TimestampType => Timestamp.valueOf(fieldValue)
        case _: DateType => Date.valueOf(fieldValue)
        case _: StringType => encode(fieldValue, fieldName)
        case _ => throw new RuntimeException(s"Unsupported data type: ${toType.typeName}")
      }
    }
  }

  private def encode(value: String, fieldName: String): String = {
    if (shouldEncode(fieldName)) {
      URLEncoder.encode(value, "UTF-8")
    } else {
      value
    }
  }

  private def shouldEncode(fieldName: String) : Boolean = {
    if (encodeFields != null && encodeFields.isDefined) {
      val toBeEncodedField = encodeFields.get.split(",")
      return toBeEncodedField.contains(fieldName)
    }

    false
  }

  private def sampleRDD: RDD[Array[String]] = {
    // Defaulting sample values to 10
    val NO_OF_SAMPLE_ROWS = 10;
    // If the record is less than 10, then the whole data is used as sample
    if (cachedResult == null) {
	    cachedResult = readNext;
	}
	var records: java.util.List[java.util.Map[String, String]] = cachedResult
    val sampleSize = if (records.size() < NO_OF_SAMPLE_ROWS) {
      records.size()
    } else {
      NO_OF_SAMPLE_ROWS
    }

    logger.debug("Sample Size : " + sampleSize)
    // Constructing RDD from records
    val sampleRowArray = new Array[Array[String]](sampleSize)
    for (i <- 0 to sampleSize - 1) {
      val row = records(i);
      val fieldArray = new Array[String](row.size())

      var fieldIndex: Int = 0
      for (column <- row) {
        fieldArray(fieldIndex) = column._2
        fieldIndex = fieldIndex + 1
      }

      sampleRowArray(i) = fieldArray
    }

    // Converting the Array into RDD
    sqlContext.sparkContext.parallelize(sampleRowArray)
  }

  private def header: Array[String] = {
    if (cachedResult == null) {
	    cachedResult = readNext;
	}
	var records: java.util.List[java.util.Map[String, String]] = cachedResult
    val firstRow = records.iterator().next()
    val header = new Array[String](firstRow.size())
    var index: Int = 0
    for (column <- firstRow) {
      header(index) = column._1
      index = index + 1
    }

    header
  }

  override def schema: StructType = {
    if (cachedResult == null) {
	    cachedResult = readNext;
	}
	var records: java.util.List[java.util.Map[String, String]] = cachedResult
    if (userSchema != null) {
      userSchema
    } else if (records == null || records.size() == 0) {
      new StructType();
    } else if (inferSchema) {
      InferSchema(sampleRDD, header)
    } else {
      // Construct the schema with all fields as String
      val firstRow = records.iterator().next()
      val structFields = new Array[StructField](firstRow.size())
      var index: Int = 0
      for (fieldEntry <- firstRow) {
        structFields(index) = StructField(fieldEntry._1, StringType, nullable = true)
        index = index + 1
      }

      StructType(structFields)
    }
  }

  override def buildScan(): RDD[Row] = {
    var records: java.util.List[java.util.Map[String, String]] = cachedResult
    val resultList = new java.util.ArrayList[RDD[Row]]
    // check cached records that we may have already read for a different method call
    if (records != null) {
      resultList.add(readResultToRDD(records))
	}
	
	if (resultSet != null) {
      while (!resultSet.isDone()) {
        resultList.add(readResultToRDD(readNext))
      }
    }
    
    // reset....  in case this is called again
    resultSet = null
    cachedResult = null
    unionRDDs(resultList)
  }
  
  private def unionRDDs(rdds: java.util.List[RDD[Row]]): RDD[Row] = {
    var workingResult: RDD[Row] = null
    var i: Int = 0
    val checkpointFrequency: Int = 100
    for (rdd <- rdds) {
      if (workingResult == null) {
        workingResult = rdd
      } else {
        val previousWorkingResult: RDD[Row] = workingResult
        workingResult = workingResult.union(rdd)
        i = i + 1
        if (i % checkpointFrequency == 0) {
          workingResult.persist(StorageLevel.MEMORY_AND_DISK_SER)
          workingResult.checkpoint()
          workingResult.take(1)
          previousWorkingResult.unpersist()
        }
      }
    }
    workingResult;
  }

  
  private def readResultToRDD(records: java.util.List[java.util.Map[String, String]]): RDD[Row] = {
    val schemaFields = schema.fields
    val rowArray = new Array[Row](records.size())
    var rowIndex: Int = 0
    for (row <- records) {
      val fieldArray = new Array[Any](schemaFields.length)
      logger.debug("Total Fields length : " + schemaFields.length)
      var fieldIndex: Int = 0
      for (fields <- schemaFields) {
        val value = fieldValue(row, fields.name)
        logger.debug("fieldValue " + value)
        fieldArray(fieldIndex) = cast(value, fields.dataType, fields.nullable, fields.name)
        fieldIndex = fieldIndex + 1
      }

      logger.debug("rowIndex : " + rowIndex)
      rowArray(rowIndex) = Row.fromSeq(fieldArray)
      rowIndex = rowIndex + 1
    }
    sqlContext.sparkContext.parallelize(rowArray)
  }

  private def fieldValue(row: java.util.Map[String, String], name: String) : String = {
    if (row.contains(name)) {
      row(name)
    } else {
      logger.debug("Value not found for " + name)
      ""
    }
  }
}
