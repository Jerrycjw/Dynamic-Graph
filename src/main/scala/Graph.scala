import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.graphstream.graph._
import org.graphstream.graph.implementations._
import org.graphstream.stream
import java.util.{StringTokenizer, Date, Calendar}
import java.lang.Long
import org.apache.spark.streaming.twitter._
import org.graphstream.algorithm._;
import java.util.Random
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.graphstream.algorithm.Dijkstra
import org.apache.commons.lang.StringUtils;

import org.graphstream.algorithm.APSP.APSPInfo;

import org.graphstream.stream.file.FileSourceDGS
import twitter4j.User
import twitter4j.Status


//*************************************
//Data set {nodeA,nodeB,time}
//input local port
//
//************************************

object Graph {
  var sc: SparkContext = _
  var spmap = Map[(String,String),Int]()
  var spdif = Map[(String,String),Int]()
  def loadTwitterKeys() = {
    System.setProperty("twitter4j.oauth.consumerKey", "m8EKmwWB6kKGMklaWir80QMBk")
    System.setProperty("twitter4j.oauth.consumerSecret", "EK9CoYFnNi7OmRgFuQU82Fp4pxMXY2uFoCsw6hKYx8OIavgH3F")
    System.setProperty("twitter4j.oauth.accessToken", "3002770147-3xFV9af3pPgozo8jpe664UiAzSCt8hfOXC2Aju1")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "exZtLFPaft1inxsKPCHzWSYPu4fatzHsgwlxHJuAmGepL")
  }
  def Check(g: Graph,NoE :String,w :Long)={
    /*Check the node or edge is active or not
    * result: nodes or edges which is out of time been deleted
    *
    * */

     if (NoE=="node"){
      if(g.getNodeSet!=null){
        val a = g.getNodeSet().iterator()

        val date = new Date()

        while (a.hasNext()){
          val elem : Node = a.next()

          val time : Date = elem.getAttribute("time")
          if(date.getTime-time.getTime>w){
            println("Delete node: %s".format(elem.getId()))
            g.removeNode(elem.getId)

          }

        }
      }


    }
    else{
       if(g.getEdgeSet!=null){
         val a = g.getEdgeSet().iterator()

         val date = new Date()

         while (a.hasNext()){
           val elem : Edge = a.next()

           val time : Date = elem.getAttribute("time")
           if(date.getTime-time.getTime>w){
             println("Delete node: %s".format(elem.getId()))
             g.removeEdge(elem.getId)

           }

         }
       }
     }


  }
  def findtk(graph : Graph, ActiveNodeSet : Set[Node]  ){



    println("\nNode size %s".format(graph.getNodeSet.size()))
    println("\nActive Node size %s".format(ActiveNodeSet.size))
    var dijkstra: Dijkstra = new Dijkstra(Dijkstra.Element.EDGE, null, "length")


    dijkstra.init(graph)
    for(elem <- ActiveNodeSet){
      dijkstra.setSource(elem)
      dijkstra.compute()

      val post = elem.getId()
      for (i <- 0 to graph.getNodeSet.size()-1){
        val node : Node = graph.getNode(i)
        val sp = dijkstra.getPath(node).getNodeCount
        val re = node.getId()
        if (spmap.contains(post,re) || spmap.contains(re,post)){
          if (spmap.contains(re,post)){
            val dif = Math.abs(spmap(re,post) - sp)
            spdif += ((re,post) -> dif)
            spmap.updated((re,post),sp)
          }
          else {
            val dif = Math.abs(spmap(post,re) - sp)
            spdif += ((post,re) -> dif)
            spmap.updated((post,re),sp)
                      }
        }
        else {
          spmap += ((post,re) -> sp)

        }
      }


    }


    val sortedmap = spdif.toList sortBy ( -_._2 ) take(10)
    sortedmap foreach {

      case (key, value) =>
        println(key + " = " + value)
    }
  }

  def createContext (checkpointDirectory : String) ={


    val duration: Duration = Seconds(3600)
    val filters = Seq("a")
    val conf = new SparkConf().setMaster("local[3]").setAppName("Twitter")
    sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint(checkpointDirectory)

    val tweets = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER_2)

    val WindowedTweets = tweets.window(Minutes(1),Seconds(10))
    var ActiveNodeSet = Set[Node]()


    //***********For using UpstateByKey Method****************************

    val User = WindowedTweets.map(rdd =>
      (1,rdd))

    def updateFunc(status: Seq[Status], addednodes: Option[Graph]): Option[Graph] ={

      val graph1 = addednodes.getOrElse(new SingleGraph("x"))

      status.foreach(s => {
        val post = s.getUser().getName()
        val re = s.getInReplyToScreenName()

          if(graph1.getNode(post) == null){
            val node : Node = graph1.addNode(post)
            node.setAttribute("time", s.getCreatedAt())
          }
        if(s.getInReplyToScreenName() != null){
          if (graph1.getNode(re) == null){
            val node2 : Node = graph1.addNode(re)
            node2.setAttribute("time", s.getCreatedAt())
          }
          if(graph1.getEdge(post+re)==null && graph1.getEdge(re+post)==null){
            val edge : Edge = graph1.addEdge(post+re, post,re)
            edge.setAttribute("time",s.getCreatedAt)
            println("Post: %s, Re: %s, Create At %s".format(post,re,s.getCreatedAt))
          }

        }


      })
      val size = graph1.getNodeSet.size()
      for (i <- 1 to size/30) {

        val random1 = Math.random() * size
        val random2 = Math.random() * size
        val int1 = random1.toInt
        val int2 = random2.toInt
        ActiveNodeSet = ActiveNodeSet.+(graph1.getNode(int1))
        ActiveNodeSet = ActiveNodeSet.+(graph1.getNode(int2))


        if (graph1.getEdge(int1.toString() + int2.toString()) == null && graph1.getEdge(int2.toString() + int1.toString()) == null) {
          val edge: Edge = graph1.addEdge(int1.toString() + int2.toString(), int1, int2)
          edge.addAttribute("length", "2")
          edge.setAttribute("time",new Date())
          println("Post: %s, Re: %s, Create At %s".format(int1, int2, new Date()))
        }
      }
      Check(graph1,"node",(100000).toLong)
      Check(graph1,"edge",(100000).toLong)
      findtk(graph1,ActiveNodeSet)
      Some(graph1)
    }

    val graph1 = User.updateStateByKey[Graph](updateFunc _)
    graph1.foreachRDD(rdd => {
      rdd.foreach{
        case(id,g) =>{
          var display = readLine("Display or not : ")
          if(display.equals("Y"))
          g.display()

        }
      }

    })
//Finding Top K shortest Path distance change


    ssc.start()

    ssc.awaitTermination(Minutes(300).milliseconds)
    ssc.awaitTermination()
    ssc.stop(true)
    if (sc != null) {
      sc.stop()
    }

  }


  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    loadTwitterKeys
    createContext("~/checkpoint")





  }
}