package com.databricks.caching.util

import scala.collection.mutable
import com.databricks.caching.util.AsciiTable.Header

/**
 * A simple textual table printer.
 *
 * For example:
 *
 * {{{
 *   val table = new AsciiTable(Header("X"), Header("Y"))
 *           .appendRow("1", "2")
 *           .appendRow("3", "4")
 *   println(table.toString())
 * }}}
 *
 * prints:
 *
 * <pre>
 * ┌───┬───┐
 * │ X │ Y │
 * ├───┼───┤
 * │ 1 │ 2 │
 * │ 3 │ 4 │
 * └───┴───┘
 * </pre>
 *
 * Note: The table is not literally ASCII, because the class is used to output arbitrary Unicode
 * characters (such as the ∞ used for formatting Dicer slices), and includes Unicode box drawing
 * characters for the borders. We have verified that the table renders correctly in the environments
 * we require.
 */
class AsciiTable(headers: Header*) {
  import AsciiTable._

  /**
   * The maximum (possibly truncated) length of any value added to each column in the table.
   * Initialized to the lengths of the header names, and updated as rows are appended.
   */
  private val columnWidths: Array[Int] = headers.map { header: Header =>
    header.name.length
  }.toArray

  /** Rows (excluding headers) added to the table using [[appendRow()]]. */
  private val rows = new mutable.ArrayBuffer[Array[String]]()

  /**
   * REQUIRES: the number of `values` matches the number of `headers`.
   *
   * Appends a row with given values to the table.
   */
  def appendRow(values: String*): this.type = {
    require(values.length == columnWidths.length)
    val truncatedValues = new Array[String](values.length)
    for (i: Int <- values.indices) {
      val maxWidth: Int = headers(i).maxWidth
      val cell: String = values(i)
      val truncatedCell: String = if (cell.length > maxWidth) {
        cell.substring(0, maxWidth - TRUNCATION_SUFFIX.length) + TRUNCATION_SUFFIX
      } else {
        cell
      }
      truncatedValues(i) = truncatedCell
      columnWidths(i) = columnWidths(i).max(truncatedCell.length)
    }
    rows += truncatedValues
    this
  }

  /** Writes the table to the given string builder. */
  def appendTo(builder: mutable.StringBuilder): Unit = {
    // Write the header row and borders.
    val headerLabels: Seq[String] = headers.map((_: Header).name)
    writeBorder(builder, Border.Top)
    writeCellRow(builder, headerLabels)
    writeBorder(builder, Border.HeaderBottom)

    for (row: Array[String] <- rows) {
      writeCellRow(builder, row.toIndexedSeq)
    }
    writeBorder(builder, Border.Bottom)
  }

  override def toString(): String = {
    val builder = new mutable.StringBuilder
    appendTo(builder)
    builder.toString()
  }

  /**
   * Writes the given `cells` to `builder`, where cells are padded to fill out the corresponding
   * column widths and are separated by the column separator (" │ ").
   *
   * For example, for `cells=Seq(1, 2)`, this would append something like "│ 1 │ 2 │"
   */
  private def writeCellRow(builder: mutable.StringBuilder, cells: Seq[String]): Unit = {
    writeRow(builder, Border.Cells, cellsOpt = Some(cells))
  }

  /**
   * Writes a border row to `builder` using the given `border` type.
   *
   * For example, for [[Border.Top]], this will append ""┌───┬───┐".
   */
  private def writeBorder(builder: mutable.StringBuilder, border: Border): Unit = {
    writeRow(builder, border, cellsOpt = None)
  }

  /**
   * Writes a row to `builder` using the given `border`, where the values of the cells are filled
   * with the padded values of the cells in by `cellsOpt`, and with empty padding otherwise.
   */
  private def writeRow(
      builder: mutable.StringBuilder,
      border: Border,
      cellsOpt: Option[Seq[String]]): Unit = {
    require(cellsOpt.isEmpty || cellsOpt.get.length == columnWidths.length)

    builder.append(border.left)
    for (i: Int <- columnWidths.indices) {
      val columnWidth: Int = columnWidths(i)
      // Append the padded value of the cell if provided, or empty padding otherwise.
      val cell: String = cellsOpt.map((_: Seq[String])(i)).getOrElse("")
      builder.append(cell.padTo(columnWidth, border.padding))
      if (i < columnWidths.length - 1) {
        builder.append(border.columnSeparator)
      } else {
        builder.append(border.right)
      }
    }
    builder.append('\n')
  }
}

object AsciiTable {
  private val TRUNCATION_SUFFIX = "..."

  /**
   * REQUIRES: `maxWidth` is at least as long as `name` and `TRUNCATION_SUFFIX`.
   *
   * A header for a column in an [[AsciiTable]].
   *
   * @param name the name of the column
   * @param maxWidth the maximum width of the column. If a column value exceeds this width, it will
   *                 be truncated to fit, with a "..." suffix.
   */
  case class Header(name: String, maxWidth: Int = 100) {
    require(maxWidth >= name.length)
    require(maxWidth >= TRUNCATION_SUFFIX.length)
  }

  /** A trait that describes the characters used to format different parts of a table border. */
  private sealed trait Border {
    def left: String
    def padding: Char
    def columnSeparator: String
    def right: String
  }
  private object Border {

    /** The [[Border]] for the top of a table, e.g. "┌───┬───┐" */
    case object Top extends Border {
      val left: String = "┌─"
      val padding: Char = '─'
      val columnSeparator: String = "─┬─"
      val right: String = "─┐"
    }

    /** The [[Border]] beneath the header of a table, e.g "├───┼───┤" */
    object HeaderBottom extends Border {
      val left: String = "├─"
      val padding: Char = '─'
      val columnSeparator: String = "─┼─"
      val right: String = "─┤"
    }

    /** The [[Border]] for the cells in a table, e.g. "│   │   │" */
    object Cells extends Border {
      val left: String = "│ "
      val padding: Char = ' '
      val columnSeparator: String = " │ "
      val right: String = " │"
    }

    /** The [[Border]] for the bottom of a table, e.g. "└───┴───┘" */
    object Bottom extends Border {
      val left: String = "└─"
      val padding: Char = '─'
      val columnSeparator: String = "─┴─"
      val right: String = "─┘"
    }
  }
}
