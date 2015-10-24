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

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.graphstream.graph._
import org.graphstream.graph.implementations._
import org.graphstream.stream
import java.util.Date
import java.lang.Long
import org.apache.spark.streaming.twitter._


//*************************************
//Data set {nodeA,nodeB,time}
//input local port
//
//************************************

object GraphStream {
  var sc: SparkContext = _
  def InputData(x: Array[String],g: SingleGraph) = {
    /* Add edges to graph
    * unsolved problem: nodes should be automatic generated
    *
    *
    * */
    g.setAutoCreate( true )
    if (x.length != 2) {println("input error")}
    else     {g.addEdge(x(0) + x(1),x(0),x(1))

    }
    println(x)
  }


  def Check(g: SingleGraph,NoE :String,w :Long)={
    /*Check the node or edge is active or not
    * result: nodes or edges which is out of time been deleted
    *
    * */

    if (NoE=="node"){
      var a = g.getNodeSet().iterator()
      var node : Node = a.next()
      val date = new Date()

      while (a.hasNext()){
        var elem : Node = a.next()
        var time : Long = Long.parseLong(elem.getAttribute("time"))

        if(date.getTime-time>w){g.removeNode(elem.getId)}
      }

    }


  }
  def loadTwitterKeys() = {
    System.setProperty("twitter4j.oauth.consumerKey", "m8EKmwWB6kKGMklaWir80QMBk")
    System.setProperty("twitter4j.oauth.consumerSecret", "EK9CoYFnNi7OmRgFuQU82Fp4pxMXY2uFoCsw6hKYx8OIavgH3F")
    System.setProperty("twitter4j.oauth.accessToken", "3002770147-3xFV9af3pPgozo8jpe664UiAzSCt8hfOXC2Aju1")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "exZtLFPaft1inxsKPCHzWSYPu4fatzHsgwlxHJuAmGepL")
  }

  def configureStreamingContext() = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Twitter")
    sc = new SparkContext(conf)
    new StreamingContext(sc, Seconds(10))
  }

  def startStream()= {
    val duration: Duration = Seconds(3600)
    val filters = Seq("Yeoman", "npm", "gruntjs", "jsconf", "nodejs", "docker", "tdd")
    val ssc = configureStreamingContext()
    val tweets = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER_2)
    // Print tweets batch count
    tweets.foreachRDD(rdd => {
      println("\nNew tweets %s:".format(rdd.count()))
    })
    //get users and followers count
    val status = tweets.map(status =>
      (status.getId, status.getInReplyToStatusId)
    )
    //print top users

    ssc.start()
    //ssc.awaitTermination(Minutes(300).milliseconds)
    ssc.awaitTermination()
    ssc.stop(true)
    if (sc != null) {
      sc.stop()
    }
  }
  def main(args: Array[String]): Unit = {





    var date = new Date()
    // Create the context with a 3 second batch size
    var starttime : Long = date.getTime()
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val filters : Array[String] = Array("ISIS","obama")
    val stream = TwitterUtils.createStream(ssc, None, filters)


    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val nodes = lines.flatMap(_.split(" "))
    /** *****process input ********/

    val graph = new SingleGraph("T1")
    date = new Date()
    var time:String = date.getTime().toString()
    nodes.persist()
    nodes.foreachRDD(rdd => {
      println(rdd.id)
      val x = rdd.collect();
      if (x.isEmpty == false){
        for (i <- 0 until 2) {//i = 0 i =1
          if (graph.getNode(x(i)) == null){
            graph.addNode(x(i))
            val node : Node = graph.getNode(x(i))
            node.setAttribute("time",time)          //****
          }
          println(x(i))
        }
        InputData(x,graph)
      }

      if(!graph.getNodeSet.isEmpty())
        Check(graph,"node",30000)


    })

    graph.display()



    ssc.start()
    ssc.awaitTermination()
  }
}


