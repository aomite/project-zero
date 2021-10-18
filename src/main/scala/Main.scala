import org.mongodb.scala._
import scala.concurrent._
import org.mongodb.scala.model._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Sorts._
import io.StdIn._
import scala.io.Source  

import example.Helpers._
import org.bson.BsonValue
import org.bson.codecs.pojo.annotations.BsonId
import scala.collection.SeqMap

object Main extends App {
  
  // Connect to server
  val client: MongoClient = MongoClient("mongodb://127.0.0.1:27017/")   // localhost: 27017

  // Reads CSV file and inserts it into a MongoDB database
  val cl: Runtime = Runtime.getRuntime(); 
  var runCommand: Process = null; 
  val command: String = "mongoimport --db test --collection mgPractice --type csv --headerline --drop C:/Users/amosc/Documents/Work/mgDemo/regional-us-daily-latest.csv"
  
  try {
    runCommand = cl.exec(command)
    println("Reading file into Database...")
    println("Successfully added file to specified Database")
  } catch {
      case e: Exception => {
        println("ERROR! Could not read CSV file")
    } 
  }

  // Get an object to the DB
  val database: MongoDatabase = client.getDatabase("test")    // use test

  // Get a Collection
  val collection: MongoCollection[Document] = database.getCollection("mgPractice")    // db.mgPractice
  
  // println("Welcome! You can use this program to see various info about the top artists and songs on Spotify")
  // println("To view the top 5 Artists type 1. To view the top 5 Songs type 2.")
  // println("Please type your selection")

  var userResponse: Int = readInt()
 
  userResponse match {
    // fail safe: collection.aggregate(Seq(group("$Artist"))).printResults()
    case 1 => collection.aggregate(Seq(Aggregates.group("$Artist", Accumulators.sum("totalStreams", "$Streams")), Aggregates.sort(descending("totalStreams")))).printResults()
    case 2 => collection.aggregate(Seq(group("$Track Name"))).printResults()
    case _ => println("Please type 1(Artist) or 2(Songs)")
  }
  
  // db.songInfo.aggregate([{$group: {_id: "$Artist", totalStreams:{$sum: "$Streams"}}}, {$sort: {totalStreams:-1}}])
  collection.aggregate(Seq(Aggregates.group("$Artist", Accumulators.sum("totalStreams", "$Streams")), Aggregates.sort(descending("totalStreams")))).printResults()

  //collection.aggregate(Seq(group("$Artist"))).printResults()
  println("Waiting....") 
}
