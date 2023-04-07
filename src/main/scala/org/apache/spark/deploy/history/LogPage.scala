package org.apache.spark.deploy.history

import java.io.File
import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, Unparsed}

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage, SparkUI, SparkUITab}
import org.apache.spark.util.Utils
import org.apache.spark.util.logging.RollingFileAppender

/** jsingh 
	This class is copy of org.apache.spark.deploy.worker.ui.LogPage.
	The render method in this class serves as end point to fetch the first set of logs for executors.
	Another copy LogPageMore of same class with different implementation for render method to fetch the logs based on initial offset

*/

private[history] class LogPage(parent: HistoryServer, logProvider: LogProvider) extends WebUIPage("logPage") with Logging {
  private val supportedLogTypes = Set("stderr", "stdout")
  private val defaultBytes = 100 * 1024
  
  val workDir=logProvider.sparkLogsHome
  
  def render(request: HttpServletRequest): Seq[Node] = {
    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val podId = Option(request.getParameter("podId"))
    val driverId = Option(request.getParameter("driverId"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)
    
    /**
     val (logDir, params, pageName) = (appId, executorId, driverId) match {
      case (Some(a), Some(p), None) =>
        //(s"${workDir}/$a/$p/", s"appId=$a&executorId=$e", s"$a/$e")
        (s"${workDir}/$a/", s"appId=$a", s"$a")
      case (None, None, Some(d)) =>
        //(s"${workDir}/$d/", s"driverId=$d", d)
        (s"${workDir}/$d/", s"driverId=$d", d)
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }
    */
    
     val (logDir, params, pageName) = (appId, podId) match {
      case (Some(a), Some(p)) =>
        //(s"${workDir}/$a/$p/", s"appId=$a&executorId=$e", s"$a/$e")
        (s"${workDir}/$a/", s"appId=$a&podId=$p", s"$a/$p")
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }
    

    val (logText, startByte, endByte, logLength) = logProvider.getLog(logDir, appId.get, podId.get, offset, byteLength)
    //val (logText, startByte, endByte, logLength) = logProvider.getLog(logDir, logType, offset, byteLength)
    //val linkToHome = <p><a href="" id="home-link">Back to Applications</a></p>  
   // val linkToExecutors = <p><a href="" id="exec-link">Back to Executors</a></p>  
    //<p><a href={worker.activeMasterWebUiUrl}>Back to Master</a></p>
    val curLogLength = endByte - startByte
    val range =
      <span id="log-data">
        Showing {curLogLength} Bytes: {startByte.toString} - {endByte.toString} of {logLength}
      </span>

    val moreButton =
      <button type="button" onclick={"loadMoreLogs()"} class="log-more-btn btn btn-secondary">
        Load More
      </button>

    val newButton =
      <button type="button" onclick={"loadNewLogs()"} class="log-new-btn btn btn-secondary">
        Load New
      </button>

    val alert =
      <div class="no-new-alert alert alert-info" style="display: none;">
        End of Log
      </div>

    val logParams = "?%s&logType=%s".format(params, logType)
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    val content =
      <div>
        {range}
        <div class="log-content" style="height:80vh; overflow:auto; padding:5px;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
      </div>

    UIUtils.basicSparkPage(request, content, logType + " log page for " + pageName)
  }

 def renderLog(request: HttpServletRequest): String = {
    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val podId = Option(request.getParameter("podId"))
    val driverId = Option(request.getParameter("driverId"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)
	
	/**
     val (logDir, params, pageName) = (appId, executorId, driverId) match {
      case (Some(a), Some(e), None) =>
        (s"${workDir}/$a/$e/", s"appId=$a&executorId=$e", s"$a/$e")
      case (None, None, Some(d)) =>
        (s"${workDir}/$d/", s"driverId=$d", d)
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }
    */
    
      val (logDir, params, pageName) = (appId, podId) match {
      case (Some(a), Some(p)) =>
        //(s"${workDir}/$a/$p/", s"appId=$a&executorId=$e", s"$a/$e")
        (s"${workDir}/$a/", s"appId=$a&podId=$p", s"$a/$p")
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (logText, startByte, endByte, logLength) = logProvider.getLog(logDir, appId.get, podId.get, offset, byteLength)
    val pre = s"==== Bytes $startByte-$endByte of $logLength of $logDir$logType ====\n"
    pre + logText
  }


}
