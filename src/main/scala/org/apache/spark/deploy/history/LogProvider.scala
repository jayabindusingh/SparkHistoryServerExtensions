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

import java.io.{File, FileNotFoundException, IOException, BufferedReader,InputStream, FileInputStream}
import java.lang.{Long => JLong}
import java.nio.file.Files
import java.util.{Date, NoSuchElementException, ServiceLoader}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, TimeUnit}
import java.util.zip.ZipOutputStream
import scala.io.Source



import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, FSDataInputStream}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.HdfsConstants
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils
//import org.apache.spark.util.logging.RollingFileAppender
import scala.util.control._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.io.{ByteStreams, Files => GFiles}
import java.util.zip.{GZIPInputStream, ZipInputStream}

/** jsingh 
	This class serves as provider of logs from various file systems based on spark.logs.home.dir setting.
	This is a custom setting introduced with this UI extension and is the place where logs collected from executors are stored.
	This class can handle local file system and also cloud buckets such as ADLS, GC, S3.
	Implementation also supports rolling logs. 
	
	Most of the methods are taken from below mentioned classes but have been modified to serve as log readers.
	org.apache.spark.deploy.history (FsHistoryProvider, SingleFileEventLogFileReader),
	org.apache.spark.util.Util,org.apache.spark.internal.logging.RollingFileAppender
	
*/

private[history] class LogProvider(conf: SparkConf) extends Logging {
	
  private val supportedLogTypes = Set("stderr", "stdout")
  private val defaultBytes = 100 * 1024
  
  val sparkLogsHome=conf.get("spark.logs.home.dir", "")
  logInfo("spark.logs.home.dir="+sparkLogsHome);
  
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private[history] val fs: FileSystem = new Path(sparkLogsHome).getFileSystem(hadoopConf)
   

   		
    def getLog(
      logDirectory: String,
      logType: String,
      offsetOption: Option[Long],
      byteLength: Int
    ): (String, Long, Long, Long) = {

    if (!supportedLogTypes.contains(logType)) {
      return ("Error: Log type must be one of " + supportedLogTypes.mkString(", "), 0, 0, 0)
    }
    
    val logDirPath:Path=new Path(logDirectory)
    if (!fs.getFileStatus(logDirPath).isDirectory) {
      return ("Error: invalid log directory " + logDirectory, 0, 0, 0)
    }
    try{
	    logInfo("Reading log file : "+logDirectory+""+logType);
	    val activeFilePath=new Path(logDirectory,logType);
	    
	     val files = RollingFileReader.getSortedRolledOverFiles(fs, logDirectory, activeFilePath)
	     logDebug(s"Sorted log files of type $logType in $logDirectory:\n${files.mkString("\n")}")
	     val fileLengths: Seq[Long] = files.map(RollingFileReader.getFileLength(fs, _, logDirectory))
	     
	      val totalLength = fileLengths.sum
	      val offset = offsetOption.getOrElse(totalLength - byteLength)
	      val startIndex = {
	        if (offset < 0) {
	          0L
	        } else if (offset > totalLength) {
	          totalLength
	        } else {
	          offset
	        }
	      }
	      val endIndex = math.min(startIndex + byteLength, totalLength)
	      logDebug(s"Getting log from $startIndex to $endIndex")
	      val logText = RollingFileReader.offsetBytes(fs:FileSystem, files, fileLengths, startIndex, endIndex)
	      logDebug(s"Got log of length ${logText.length} bytes")
	      (logText, startIndex, endIndex, totalLength)
    } catch {
      case e: Exception =>
        logError(s"Error getting $logType logs from directory $logDirectory", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
   
	
	}  
    
  

}



/**
 * Companion object to [[org.apache.spark.util.logging.RollingFileAppender]]. Defines
 * names of configurations that configure rolling file appenders.
 */
private[history] object RollingFileReader  extends Logging{
  val DEFAULT_BUFFER_SIZE = 8192

  val GZIP_LOG_SUFFIX = ".gz"
  private val UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE_CONF =100
 

  /**
   * Get the sorted list of rolled over files. This assumes that the all the rolled
   * over file names are prefixed with the `activeFileName`, and the active file
   * name has the latest logs. So it sorts all the rolled over logs (that are
   * prefixed with `activeFileName`) and appends the active file
   */
  def getSortedRolledOverFiles(fs: FileSystem, directory: String, activeFilePath: Path): Seq[FileStatus] = {
    val path=new Path(directory)
     logInfo("Log Dir="+directory)
    val rolledOverFiles = fs.listStatus(path).filter { fileStatus =>
      val fileName = fileStatus.getPath().getName()
      logInfo(fileName)
      fileName.startsWith(activeFilePath.getName()) && fileName != activeFilePath.getName()
    }
 //   val activeFile = {
  //    if (fs.isFile(activeFilePath)) Some(fs.listStatus(activeFilePath)) else None
 //   }
    rolledOverFiles.sortBy(_.getPath().getName().stripSuffix(GZIP_LOG_SUFFIX))++fs.listStatus(activeFilePath)
  }
  
  private var compressedLogFileLengthCache: LoadingCache[String, java.lang.Long] = null
  
  private def getCompressedLogFileLengthCache(fs: FileSystem
  			): LoadingCache[String, java.lang.Long] = this.synchronized {
    if (compressedLogFileLengthCache == null) {
      val compressedLogFileLengthCacheSize = UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE_CONF
      compressedLogFileLengthCache = CacheBuilder.newBuilder()
        .maximumSize(compressedLogFileLengthCacheSize)
        .build[String, java.lang.Long](new CacheLoader[String, java.lang.Long]() {
        override def load(file: String): java.lang.Long = {
          getCompressedFileLength(fs,file)
        }
      })
    }
    compressedLogFileLengthCache
  }
  
  
   /**
   * Return the file length, if the file is compressed it returns the uncompressed file length.
   * It also caches the uncompressed file size to avoid repeated decompression. 
   */
  def getFileLength(fs: FileSystem, fileStatus: FileStatus, logDirectory : String): Long = {
    if (fileStatus.getPath.getName.endsWith(".gz")) {
      getCompressedLogFileLengthCache(fs).get(logDirectory+fileStatus.getPath.getName)
    } else {
      fileStatus.getLen
    }
  }

  /** Return uncompressed file length of a compressed file. */
  private def getCompressedFileLength(fs:FileSystem, file: String): Long = {
    var gzInputStream: GZIPInputStream = null
    try {
      // Uncompress .gz file to determine file size.
      var fileSize = 0L
      //gzInputStream = new GZIPInputStream(new FileInputStream(fs.open(new Path(file))))
      gzInputStream = new GZIPInputStream(fs.open(new Path(file)))
      //gzInputStream = new GZIPInputStream(new FileInputStream(file))
      val bufSize = 1024
      val buf = new Array[Byte](bufSize)
      var numBytes = ByteStreams.read(gzInputStream, buf, 0, bufSize)
      while (numBytes > 0) {
        fileSize += numBytes
        numBytes = ByteStreams.read(gzInputStream, buf, 0, bufSize)
      }
      fileSize
    } catch {
      case e: Throwable =>
        logError(s"Cannot get file length of ${file}", e)
        throw e
    } finally {
      if (gzInputStream != null) {
        gzInputStream.close()
      }
    }
  }
  
  
  /** Return a string containing part of a file from byte 'start' to 'end'. */
  def offsetBytes(fs: FileSystem, path: Path, length: Long, start: Long, end: Long): String = {
    //val file = new File(path)
    val in:InputStream=fs.open(path);
    val effectiveEnd = math.min(length, end)
    val effectiveStart = math.max(0, start)
    val buff = new Array[Byte]((effectiveEnd-effectiveStart).toInt)
    val stream = if (path.getName.endsWith(".gz")) {
      new GZIPInputStream(in)
    } else {
    	in
    }

    try {
      ByteStreams.skipFully(stream, effectiveStart)
      ByteStreams.readFully(stream, buff)
    } finally {
      stream.close()
    }
    Source.fromBytes(buff).mkString
  }

  /**
   * Return a string containing data across a set of files. The `startIndex`
   * and `endIndex` is based on the cumulative size of all the files take in
   * the given order. See figure below for more details.
   */
  def offsetBytes(fs: FileSystem, files: Seq[FileStatus], fileLengths: Seq[Long], start: Long, end: Long): String = {
    assert(files.length == fileLengths.length)
    val startIndex = math.max(start, 0)
    val endIndex = math.min(end, fileLengths.sum)
    val fileToLength = files.zip(fileLengths).toMap
    logDebug("Log files: \n" + fileToLength.mkString("\n"))

    val stringBuffer = new StringBuffer((endIndex - startIndex).toInt)
    var sum = 0L
    files.zip(fileLengths).foreach { case (file, fileLength) =>
      val startIndexOfFile = sum
      val endIndexOfFile = sum + fileToLength(file)
      logDebug(s"Processing file $file, " +
        s"with start index = $startIndexOfFile, end index = $endIndex")

      /*
                                      ____________
       range 1:                      |            |
                                     |   case A   |

       files:   |==== file 1 ====|====== file 2 ======|===== file 3 =====|

                     |   case B  .       case C       .    case D    |
       range 2:      |___________.____________________.______________|
       */

      if (startIndex <= startIndexOfFile  && endIndex >= endIndexOfFile) {
        // Case C: read the whole file
        stringBuffer.append(offsetBytes(fs, file.getPath, fileLength, 0, fileToLength(file)))
      } else if (startIndex > startIndexOfFile && startIndex < endIndexOfFile) {
        // Case A and B: read from [start of required range] to [end of file / end of range]
        val effectiveStartIndex = startIndex - startIndexOfFile
        val effectiveEndIndex = math.min(endIndex - startIndexOfFile, fileToLength(file))
        stringBuffer.append(offsetBytes(fs,
          file.getPath, fileLength, effectiveStartIndex, effectiveEndIndex))
      } else if (endIndex > startIndexOfFile && endIndex < endIndexOfFile) {
        // Case D: read from [start of file] to [end of require range]
        val effectiveStartIndex = math.max(startIndex - startIndexOfFile, 0)
        val effectiveEndIndex = endIndex - startIndexOfFile
        stringBuffer.append(offsetBytes(fs,
          file.getPath, fileLength, effectiveStartIndex, effectiveEndIndex))
      }
      sum += fileToLength(file)
      logDebug(s"After processing file $file, string built is ${stringBuffer.toString}")
    }
    stringBuffer.toString
  }
  
}