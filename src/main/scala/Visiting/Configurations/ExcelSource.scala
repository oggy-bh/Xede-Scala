package Visiting.Configurations

import Visiting.Components.{SourceConfig, SourceConfigVisitor}

case class ExcelSource(excelRange: ExcelRange, hasHeader: Boolean, headerColumns: Option[List[String]]) extends SourceConfig {
  override def accept[TOut](visitor: SourceConfigVisitor[TOut]): TOut = visitor.Visit(this)
}

/**
 * Data Addresses
  As you can see in the examples above, the location of data to read or write can be specified with the dataAddress option.
    Currently the following address styles are supported:

    B3: Start cell of the data. Reading will return all rows below and all columns to the right. Writing will start here and use as many columns and rows as required.

    B3:F35: Cell range of data. Reading will return only rows and columns in the specified range.
            Writing will start in the first cell (B3 in this xede-scala) and use only the specified columns and rows.
            If there are more rows or columns in the DataFrame to write, they will be truncated. Make sure this is what you want.

    'My Sheet'!B3:F35: Same as above, but with a specific sheet.

    MyTable[#All]: Table of data. Reading will return all rows and columns in this table.
            Writing will only write within the current range of the table. No growing of the table will be performed. PRs to change this are welcome.

 * @param sheet
 * @param startCell
 * @param endCell
 */
case class ExcelRange(sheet: Option[String], startCell: Option[String] = Some("A1"), endCell: Option[String]) {
  def GetDataAddress(): String = {
    sheet.fold("")(x => s"'$x'!") + startCell.get + endCell.fold("")(x => s":$x") // <sheet>!<start>:<end>
  }
}
