package xede

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object RenameColumns {
  def rename(dataFrame: DataFrame): DataFrame = {
    val renamedColumns = dataFrame.columns.map(originalName => col("`" + originalName + "`").alias(transformColumnName(originalName)))
    dataFrame.select(renamedColumns:_*)
  }

  private def transformColumnName(columnName: String): String = {
    var col=columnName.trim.toLowerCase()
      .replace('\\', '_')
      .replace('/', '_')
      .replace(' ', '_')
      .replace('"', '_')
      .replaceAll("\\s", "_")
      .replace("#", "_num")
      .replace("&", "_")
      .replace("%", "pct")
      .replace("*", "_")
      .replace("'", "_")
      .replace(",", "_")
      .replace(";", "_")
      .replace(".", "_")
      .replace("(", "_")
      .replace(")", "_")
      .replace("{", "_")
      .replace("}", "_")
      .replace("-", "_")
      .replace("__", "_")
      .replace("__", "_")
      .replace("__", "_")
      .replace("__", "_")

    while (col.endsWith("_"))
      col=col.substring(0, col.length-1)
    while (col.startsWith("_"))
      col=col.substring(1)
    col
  }
}
