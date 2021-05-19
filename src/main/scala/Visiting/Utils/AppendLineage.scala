package Visiting.Utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.util.{Date}

object AppendLineage {
  // todo: this seems to have changed https://brighthealthplan.atlassian.net/wiki/spaces/DI/pages/2238350147/Data%2BLineage%2BData%2BProvenance%2BGuidelines
  //  seek guidance for implementing this.
  def appendLineageToDf(source: String, df: DataFrame) = {
    val encoder = java.security.MessageDigest.getInstance("SHA1")
    val hashCode = BigInt(1, encoder.digest(source.getBytes())).toString(36).toUpperCase
    df
      .withColumn("meta_lineage_id", lit(hashCode))
      .withColumn("meta_source_system_name", lit("adhoc"))
      .withColumn("meta_source_name", lit(source))
      .withColumn("meta_lineage_datetime", lit(new Date().toString))
      .withColumn("meta_provenance_id", lit("rawToSource"))
      .withColumn("meta_provenance_version", lit("1.0.0-SNAPSHOT"))
  }
}
