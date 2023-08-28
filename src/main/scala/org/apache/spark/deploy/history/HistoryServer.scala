/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history

import com.o9solutions.spark.authenticator.AuthenticatedRequest

import java.util.NoSuchElementException
import java.util.zip.ZipOutputStream
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.util.control.NonFatal
import scala.xml.Node

import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.History
import org.apache.spark.internal.config.UI._
import org.apache.spark.status.api.v1.{ApiRootResource, ApplicationInfo, UIRoot}
import org.apache.spark.ui.{SparkUI, UIUtils, WebUI}
import org.apache.spark.util.{ShutdownHookManager, SystemClock, Utils}
import org.apache.spark.ui.JettyUtils._



/**
 * A web server that renders SparkUIs of completed applications.
 *
 * For the standalone mode, MasterWebUI already achieves this functionality. Thus, the
 * main use case of the HistoryServer is in other deploy modes (e.g. Yarn or Mesos).
 *
 * The logging directory structure is as follows: Within the given base directory, each
 * application's event logs are maintained in the application's own sub-directory. This
 * is the same structure as maintained in the event log write code path in
 * EventLoggingListener.
 */
class HistoryServer(
    conf: SparkConf,
    provider: ApplicationHistoryProvider,
    securityManager: SecurityManager,
    port: Int)
  extends WebUI(securityManager, securityManager.getSSLOptions("historyServer"),
    port, conf, name = "HistoryServerUI",
    // Usually, a History Server stores plenty of event logs for various applications and users
    // Comparing to Spark LiveUI which is generally for per application usage, it needs more
    // threads to increase concurrency to handle request from different users and clients.
    poolSize = 1000)
  with Logging with UIRoot with ApplicationCacheOperations {

  // How many applications to retain
  private val retainedApplications = conf.get(History.RETAINED_APPLICATIONS)

  // How many applications the summary ui displays
  private[history] val maxApplications = conf.get(HISTORY_UI_MAX_APPS);

  // application
  private val appCache = new ApplicationCache(this, retainedApplications, new SystemClock())

  // and its metrics, for testing as well as monitoring
  val cacheMetrics = appCache.metrics
  
  val proxyBase=conf.get("spark.ui.proxyBase", "")
   logInfo("***** Custom Class Loaded proxyBase = " + proxyBase)
 
 //jsingh 8/26/2023 - aded this setting to allow switching off tenant filtering which may be helpful for QA and internal environments
 // default value if not set will be treated as enabled
 val tenantFiltering=conf.get("spark.ui.tenantFiltering", "enabled")
 logInfo("***** Custom Class Loaded tenantFiltering = " + tenantFiltering)
 
  private val loaderServlet = new HttpServlet {
    protected override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
	 
	 	
	  //get authenticated user and tenantinfo
	  val tenantId= req.getAttribute("TenantId").asInstanceOf[String]
	  logDebug("** Tenant Filtering TenantId: " + tenantId);
      
      res.setContentType("text/html;charset=utf-8")

      // Parse the URI created by getAttemptURI(). It contains an app ID and an optional
      // attempt ID (separated by a slash).
      val parts = Option(req.getPathInfo()).getOrElse("").split("/")
      
      logInfo("** Custom : " + req.getPathInfo()+" "+parts.length)
      
      if (parts.length < 2) {
        res.sendRedirect("/")
      }
		
      val appId = parts(1)
      
   
      
      var shouldAppendAttemptId = false
      val attemptId = if (parts.length >= 3) {
        Some(parts(2))
      } else {
        val lastAttemptId = provider.getApplicationInfo(appId).flatMap(_.attempts.head.attemptId)
        if (lastAttemptId.isDefined) {
          shouldAppendAttemptId = true
          lastAttemptId
        } else {
          None
        }
      }
      
      //jsingh 8/26/2023 - added for tenant level app filtering
      if (tenantFiltering=="enabled"){
	      try{
		    if(!provider.getApplicationInfo(appId).get.name.startsWith(tenantId)){
		      	logDebug("Failed authorization");
		      	val msg = <div class="row">Application {appId} is forbidden.</div>
		        res.setStatus(HttpServletResponse.SC_FORBIDDEN)
		        UIUtils.basicSparkPage(req, msg, "Forbidden").foreach { n =>
		          res.getWriter().write(n.toString)
		        }
		         return
		    }
	      }catch {
	      		case _: NoSuchElementException =>
	            	logDebug("**** inside catch exception *****");
	            	val msg = <div class="row">Application {appId} not found.</div>
			        res.setStatus(HttpServletResponse.SC_NOT_FOUND)
			        UIUtils.basicSparkPage(req, msg, "Not Found").foreach { n =>
			          res.getWriter().write(n.toString)
			        }
			        
			         return
	        }
	   }
      
	  // Since we may have applications with multiple attempts mixed with applications with a
      // single attempt, we need to try both. Try the single-attempt route first, and if an
      // error is raised, then try the multiple attempt route.
      if (!loadAppUi(appId, None) && (!attemptId.isDefined || !loadAppUi(appId, attemptId))) {
        val msg = <div class="row">Application {appId} not found.</div>
        res.setStatus(HttpServletResponse.SC_NOT_FOUND)
        UIUtils.basicSparkPage(req, msg, "Not Found").foreach { n =>
          res.getWriter().write(n.toString)
        }
        return
      }
	  // Note we don't use the UI retrieved from the cache; the cache loader above will register
      // the app's UI, and all we need to do is redirect the user to the same URI that was
      // requested, and the proper data should be served at that point.
      // Also, make sure that the redirect url contains the query string present in the request.
      val redirect = if (shouldAppendAttemptId) {
        req.getRequestURI.stripSuffix("/") + "/" + attemptId.get
      } else {
        req.getRequestURI
      }
      // this is fix where if proxyBase is set, it was requiring to click twice on job name
      // before displaying details of the job
      // jsingh 8/25/2023 - striping of proxy base not required after removing use of reverseProxy hence commenting this fix
      //var redirect_new=redirect.stripPrefix(proxyBase)
      val query = Option(req.getQueryString).map("?" + _).getOrElse("")
      //logInfo("*** Reload Issue : proxyBase="+proxyBase+" redirect="+redirect+"redirect_new="+redirect_new+" query="+ query)
      //res.sendRedirect(res.encodeRedirectURL(redirect_new + query))
       res.sendRedirect(res.encodeRedirectURL(redirect + query))
    }
	// SPARK-5983 ensure TRACE is not supported
    protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
      res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
    }
    
  }

  override def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T = {
    appCache.withSparkUI(appId, attemptId)(fn)
  }

  override def checkUIViewPermissions(appId: String, attemptId: Option[String],
      user: String): Boolean = {
    provider.checkUIViewPermissions(appId, attemptId, user)
  }

  initialize()

  /**
   * Initialize the history server.
   *
   * This starts a background thread that periodically synchronizes information displayed on
   * this UI with the event logs in the provided base directory.
   */
  def initialize(): Unit = {
    
    var logProvider:LogProvider=new LogProvider(conf)
      
    attachHandler(createServletHandler(proxyBase+"/logPage",
      (request: HttpServletRequest) => (new LogPage(this, logProvider)).render(request),conf))
    
    // attachHandler(createServletHandler(proxyBase+"/logPage",
    //  (request: HttpServletRequest) => (new LogPage(this, logProvider)).render(request),conf))
    
    attachHandler(createServletHandler(proxyBase+"/log",
      (request: HttpServletRequest) => (new LogPage(this, logProvider)).renderLog(request),conf))
      
    
    addStaticHandler("org/apache/spark/ui/custom",proxyBase+"/custom")
     
    attachPage(new HistoryPage(this))

      val apiHandler:ServletContextHandler =ApiRootResource.getServletHandler(this)
      apiHandler.setContextPath(proxyBase+"/api")
      attachHandler(apiHandler)
  

    addStaticHandler(SparkUI.STATIC_RESOURCE_DIR,proxyBase+"/static")

    val contextHandler = new ServletContextHandler
    contextHandler.setContextPath(proxyBase+HistoryServer.UI_PATH_PREFIX)
    contextHandler.addServlet(new ServletHolder(loaderServlet), "/*")
    attachHandler(contextHandler)
    
     logInfo("***** Custom Class Loaded - initialize *******")
  }

  /** Bind to the HTTP server behind this web interface. */
  override def bind(): Unit = {
    super.bind()
  }

  /** Stop the server and close the file system. */
  override def stop(): Unit = {
    super.stop()
    provider.stop()
  }

  /** Attach a reconstructed UI to this server. Only valid after bind(). */
  override def attachSparkUI(
      appId: String,
      attemptId: Option[String],
      ui: SparkUI,
      completed: Boolean): Unit = {
    assert(serverInfo.isDefined, "HistoryServer must be bound before attaching SparkUIs")
    ui.getHandlers.foreach { handler =>
       handler.setContextPath(proxyBase+handler.getContextPath)
      serverInfo.get.addHandler(handler, ui.securityManager)
    }
  }

  /** Detach a reconstructed UI from this server. Only valid after bind(). */
  override def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit = {
    assert(serverInfo.isDefined, "HistoryServer must be bound before detaching SparkUIs")
    ui.getHandlers.foreach(detachHandler)
    provider.onUIDetached(appId, attemptId, ui)
  }

  /**
   * Get the application UI and whether or not it is completed
   * @param appId application ID
   * @param attemptId attempt ID
   * @return If found, the Spark UI and any history information to be used in the cache
   */
  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    provider.getAppUI(appId, attemptId)
  }

  /**
   * Returns a list of available applications, in descending order according to their end time.
   *
   * @return List of all known applications.
   */
  def getApplicationList(): Iterator[ApplicationInfo] = {
   
    logDebug("***** Custom Class Loaded tenantFiltering = " + tenantFiltering)
	// jsingh 8/26/2023 return empty list and put a info message is tenant filtering is enabled to avoid any uncaught call to this method
	
	throw new Exception("Unauthorized Access, Review Tenant Level filtering")
	//provider.getListing()
  
  }
  
  
   /**
   * Returns a list of available applications, in descending order according to their end time and filtered by the tenantId.
   *
   * @return List of filtered  applications.
   */
  def getFilteredApplicationList(tenantId: String): Iterator[ApplicationInfo] = {
   
    
    if (tenantFiltering=="enabled"){
		  val filteredApplications = provider.getListing().filter { appInfo =>
		  val appName = appInfo.name
		  logDebug(appName)
		  appName.startsWith(tenantId)
		 }
      	filteredApplications
      } else {
      	provider.getListing()
      }
  	
  }

  def getEventLogsUnderProcess(): Int = {
    provider.getEventLogsUnderProcess()
  }

  def getLastUpdatedTime(): Long = {
    provider.getLastUpdatedTime()
  }

  def getApplicationInfoList(tenantId : String) : Iterator[ApplicationInfo] = {
    logDebug("**** Tenant Filtering TenantId="+tenantId)
    //getApplicationList()
    getFilteredApplicationList(tenantId)
  }

  def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    provider.getApplicationInfo(appId)
  }

  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {
    provider.writeEventLogs(appId, attemptId, zipStream)
  }

  /**
   * @return html text to display when the application list is empty
   */
  def emptyListingHtml(): Seq[Node] = {
    provider.getEmptyListingHtml()
  }

  /**
   * Returns the provider configuration to show in the listing page.
   *
   * @return A map with the provider's configuration.
   */
  def getProviderConfig(): Map[String, String] = provider.getConfig()

  /**
   * Load an application UI and attach it to the web server.
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return true if the application was found and loaded.
   */
  private def loadAppUi(appId: String, attemptId: Option[String]): Boolean = {
    try {
      appCache.withSparkUI(appId, attemptId) { _ =>
        // Do nothing, just force the UI to load.
      }
      true
    } catch {
      case NonFatal(e: NoSuchElementException) =>
        false
    }
  }

  /**
   * String value for diagnostics.
   * @return a multi-line description of the server state.
   */
  override def toString: String = {
    s"""
      | History Server;
      | provider = $provider
      | cache = $appCache
    """.stripMargin
  }
}

/**
 * The recommended way of starting and stopping a HistoryServer is through the scripts
 * start-history-server.sh and stop-history-server.sh. The path to a base log directory,
 * as well as any other relevant history server configuration, should be specified via
 * the $SPARK_HISTORY_OPTS environment variable. For example:
 *
 *   export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/tmp/spark-events"
 *   ./sbin/start-history-server.sh
 *
 * This launches the HistoryServer as a Spark daemon.
 */
object HistoryServer extends Logging {
  private val conf = new SparkConf

  val UI_PATH_PREFIX = "/history"

  def main(argStrings: Array[String]): Unit = {
    Utils.initDaemon(log)
    new HistoryServerArguments(conf, argStrings)
    initSecurity()
    val securityManager = createSecurityManager(conf)

    val providerName = conf.get(History.PROVIDER)
      .getOrElse(classOf[FsHistoryProvider].getName())
    val provider = Utils.classForName[ApplicationHistoryProvider](providerName)
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)

    val port = conf.get(History.HISTORY_SERVER_UI_PORT)

    val server = new HistoryServer(conf, provider, securityManager, port)
    server.bind()
    provider.start()

    ShutdownHookManager.addShutdownHook { () => server.stop() }

    // Wait until the end of the world... or if the HistoryServer process is manually stopped
    while(true) { Thread.sleep(Int.MaxValue) }
  }

  /**
   * Create a security manager.
   * This turns off security in the SecurityManager, so that the History Server can start
   * in a Spark cluster where security is enabled.
   * @param config configuration for the SecurityManager constructor
   * @return the security manager for use in constructing the History Server.
   */
  private[history] def createSecurityManager(config: SparkConf): SecurityManager = {
    if (config.getBoolean(SecurityManager.SPARK_AUTH_CONF, false)) {
      logDebug(s"Clearing ${SecurityManager.SPARK_AUTH_CONF}")
      config.set(SecurityManager.SPARK_AUTH_CONF, "false")
    }

    if (config.get(ACLS_ENABLE)) {
      logInfo(s"${ACLS_ENABLE.key} is configured, " +
        s"clearing it and only using ${History.HISTORY_SERVER_UI_ACLS_ENABLE.key}")
      config.set(ACLS_ENABLE, false)
    }

    new SecurityManager(config)
  }

  def initSecurity(): Unit = {
    // If we are accessing HDFS and it has security enabled (Kerberos), we have to login
    // from a keytab file so that we can access HDFS beyond the kerberos ticket expiration.
    // As long as it is using Hadoop rpc (hdfs://), a relogin will automatically
    // occur from the keytab.
    if (conf.get(History.KERBEROS_ENABLED)) {
      // if you have enabled kerberos the following 2 params must be set
      val principalName = conf.get(History.KERBEROS_PRINCIPAL)
        .getOrElse(throw new NoSuchElementException(History.KERBEROS_PRINCIPAL.key))
      val keytabFilename = conf.get(History.KERBEROS_KEYTAB)
        .getOrElse(throw new NoSuchElementException(History.KERBEROS_KEYTAB.key))
      SparkHadoopUtil.get.loginUserFromKeytab(principalName, keytabFilename)
    }
  }

  private[history] def getAttemptURI(appId: String, attemptId: Option[String]): String = {
    val attemptSuffix = attemptId.map { id => s"/$id" }.getOrElse("")
     s"${HistoryServer.UI_PATH_PREFIX}/${appId}${attemptSuffix}"
  }

}