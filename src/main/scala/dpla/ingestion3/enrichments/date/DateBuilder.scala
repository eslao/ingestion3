package dpla.ingestion3.enrichments.date

import dpla.ingestion3.model.DplaMapData.ZeroToOne
import dpla.ingestion3.model._

import java.util.regex.Pattern

/**
  *
  */
class DateBuilder {

  private val yearRangeRegex = "^[\\s]*[0-9]{4}[\\s]*-[\\s]*[0-9]{4}[\\s]*$"
  private val yearRegex = "^[0-9]{4}$"
  private val yyyyMmDdRegex = "(([\\d]{4})-[\\d]{2}-[\\d]{2})[\\s]*-[\\s]*(([\\d]{4})-[\\d]{2}-[\\d]{2})"
  /**
    * Attempts to enrich an original date value by identifying begin and end year values
    * from the original string
    * @param originalSourceDate Un-enriched date value
    * @return EdmTimeSpan
    */
  def generateBeginEnd(originalSourceDate: ZeroToOne[String]): EdmTimeSpan = {
    originalSourceDate match {
      case None => emptyEdmTimeSpan
      case Some(d) =>
        EdmTimeSpan(
          originalSourceDate = Some(d),
          prefLabel = Some(d),
          begin = createBegin(d),
          end = createEnd(d)
        )
    }
  }

  /**
    * Identifies a begin date
    *
    * @param str Original date value
    * @return Option[String]
    */
  def createBegin(str: String): Option[String] = str match {
    case `str` if str.matches(yearRegex) => Some(str)
    case `str` if str.matches(yearRangeRegex) => Option(str.split("-").head.trim)
    case `str` if str.matches(yyyyMmDdRegex) =>
      val pattern = Pattern.compile(yyyyMmDdRegex)
      val matches = pattern.matcher(str)
      if(matches.find()) {
        Option(matches.group(2))
      } else
        None
    case _ => None
  }

  /**
    * Identifies an end date
    *
    * @param str Original date value
    * @return Option[String]
    */
  def createEnd(str: String): Option[String] = str match {
      case `str` if str.matches(yearRegex) => Some(str)
      case `str` if str.matches(yearRangeRegex) => Option(str.split("-").last.trim)
      case `str` if str.matches(yyyyMmDdRegex) =>
        val pattern = Pattern.compile(yyyyMmDdRegex)
        val matches = pattern.matcher(str)
        if(matches.find()) {
          Option(matches.group(4))
        } else
          None
      case _ => None
    }
}
