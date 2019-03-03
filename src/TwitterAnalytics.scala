package com.genp.sm

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

object TwitterAnalytics {

  val config = new SparkConf().setMaster("local[*]").setAppName("TwitterAnalytics")
  val sc = new SparkContext(config)
  sc.setLogLevel("ERROR")
  val ssc = new StreamingContext(sc, Seconds(10))

  def main(args: Array[String]): Unit = {

    System.setProperty("twitter4j.oauth.consumerKey", "key")
    System.setProperty("twitter4j.oauth.consumerSecret", "secret")
    System.setProperty("twitter4j.oauth.accessToken", "token")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "toksec")

    val stream = TwitterUtils.createStream(ssc, None)

    //Location of the users tweeted in last 90 seconds
    val loc = stream.window(Seconds(60)).flatMap(status => Option(status.getPlace).map(pl => { s"${pl.getFullName}" }))
    loc.foreachRDD( r => {
      println("Location of the users tweeted")
      println("#"*100)
      r.foreach(loc => println(s"LOCATION::$loc \n"+"-"*100))
      println("#"*100)
    })

    displayTopObjects(stream)
    //Display top tweets based on retweet counts
    topTweetsBasedOnRetweetCount(stream)
    //Display top users based on followers count
    topUsersBasedOnFollowersCount(stream)
    //Display sentiments of tweets with categories POSITIVE, NEGATIVE, NEUTRAL
    displaySentiments(stream)

    // Start Twitter stream
    ssc.start()
    ssc.awaitTermination()
  }

  def displayTopObjects(stream: ReceiverInputDStream[Status]): Unit ={
    //Display top 3 locations where tweet was posted
    val places = stream.flatMap(status => Option(status.getPlace).map(pl => { s"${pl.getCountryCode}::${pl.getFullName}" }))
    display(places, 90,"Places", 3)

    //Display top 3 users where tweet was posted
    val users = stream.flatMap(status => Option(status.getUser).map(usr => { s"${usr.getName}" }))
    display(users, 90,"Users", 3)

    //Display top 3 hashtags where tweet was posted
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    display(hashTags, 90,"Hashtags", 3)
  }

  //Display top topNum objects in last window seconds
  def display(objects: DStream[String], window: Int, objName:String, topNum: Int): Unit ={
    val objCounts = objects.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(window))
      .map { case (obj, count) => (count, obj) }
      .transform(_.sortByKey(false))

    // Print top topNum popular objects
    objCounts.foreachRDD(rdd => {
      val topList = rdd.take(topNum)
      println(s"Top $topNum $objName in last $window seconds (${rdd.count} total)")
      println("#"*100)
      topList.foreach { case (count, obj) => println(s"$obj \n"+"-"*100)}
      println("#"*100)
    })
  }

  //Display top tweets based on retweet counts
  def topTweetsBasedOnRetweetCount(stream: ReceiverInputDStream[Status]): Unit ={
    val tweets = stream.window(Seconds(180)).flatMap(status => Option(status.getRetweetedStatus)).map(rtwt => (Option(rtwt.getText).getOrElse(""), Option(rtwt.getRetweetCount).getOrElse(0)))
      .map { case (tweet, count) => (count, tweet) }
      .transform(_.sortByKey(false))

    tweets.foreachRDD(rdd => {
      val topList = rdd.take(3)
      println("Top 3 tweets by retweet count in last 180 seconds (%s total):".format(rdd.count()))
      println("#"*100)
      topList.foreach { case (count, tweet) => println(s"$tweet (Retweet_Count::$count)\n"+"-"*100) }
      println("#"*100)
    })
  }

  //Display top users based on followers count
  def topUsersBasedOnFollowersCount(stream: ReceiverInputDStream[Status]): Unit ={
    val userBFs = stream.window(Seconds(180)).flatMap(status => Option(status.getUser).map(usr => { s"${usr.getName}~!!~${usr.getFollowersCount}" }))
    val userCunts180Sec = userBFs.map(usr => (usr.split("~!!~")(0),usr.split("~!!~")(1).toInt ))
      .map { case (user, count) => (count, user) }
      .transform(_.sortByKey(false))

    userCunts180Sec.foreachRDD(rdd => {
      val topList = rdd.take(3)
      println(s"Top 3 users in last 180 seconds (total::${rdd.count})")
      println("#"*100)
      topList.foreach { case (count, user) => println(s"$user (Followers_Count::$count)\n"+"-"*100) }
      println("#"*100)
    })
  }

  //Display sentiments of tweets with categories POSITIVE, NEGATIVE, NEUTRAL
  def displaySentiments(stream: ReceiverInputDStream[Status]): Unit ={
    val tweets = stream.window(Seconds(60)).map{ t =>
      val sentiment = SentimentAnalysisUtils.detectSentiment(t.getText)
      (t.getText, sentiment)
    }
    println("Sentiments of the tweets")
    println("#"*100)
    tweets.filter{ case(twt, sentm) => Seq(SentimentAnalysisUtils.POSITIVE, SentimentAnalysisUtils.NEGATIVE, SentimentAnalysisUtils.NEUTRAL).contains(sentm)}
      .foreachRDD(t => t.foreach{ case(twt, sntmt) => println(s"TWEET:::$twt  SENTIMENT:::$sntmt\n"+"-"*200)})
    println("#"*100)
  }

}
