package org.apache.spark.ui.exec

import java.io.File
import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, Unparsed}

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage, SparkUI, SparkUITab}
import org.apache.spark.util.Utils
import org.apache.spark.util.logging.RollingFileAppender

private[ui] class LogPageMore(parent: SparkUITab, logProvider: LogProvider) extends WebUIPage("log") with Logging {
  private val supportedLogTypes = Set("stderr", "stdout")
  private val defaultBytes = 100 * 1024
  val workDir=logProvider.sparkLogsHome  
  
  def render(request: HttpServletRequest): Seq[Node] = {
    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val driverId = Option(request.getParameter("driverId"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)

     val (logDir, params, pageName) = (appId, executorId, driverId) match {
      case (Some(a), Some(e), None) =>
        (s"${workDir}/$a/$e/", s"appId=$a&executorId=$e", s"$a/$e")
      case (None, None, Some(d)) =>
        (s"${workDir}/$d/", s"driverId=$d", d)
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (logText, startByte, endByte, logLength) = logProvider.getLog(logDir, logType, offset, byteLength)
    val pre = s"==== Bytes $startByte-$endByte of $logLength of $logDir$logType ====\n"
    <div>{pre + logText}</div>
  }


}
