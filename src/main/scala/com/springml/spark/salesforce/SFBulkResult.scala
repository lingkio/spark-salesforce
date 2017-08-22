package com.springml.spark.salesforce

class SFBulkResult (
    val overallSuccess: Boolean,
    val successfulRecords: Int,
    val failedRecords: Int) extends Serializable {
    
}