import org.mongodb.scala._
import scala.concurrent._
import org.mongodb.scala.model._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Projections._
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
  
  println("Welcome! You can use this program to see various info about the top artists and songs on Spotify")
  println("To view information about Artists type 1. For information regarding Songs type 2.")
  println("Please type your selection")

  var userResponse: Int = readInt()
 
  userResponse match {
    // fail safe reference: collection.aggregate(Seq(group("$Artist"))).printResults()
    case 1 => {
      println("To view top 5 Artist type 1. For top Artist by Total Streams type 2. For top Artist by Average Streams type 3. To EXIT type 4")
      var secondUserResponse: Int = readInt()

      secondUserResponse match{
        case 1 => collection.aggregate(Seq(project(fields(include("Position", "Artist"), excludeId())), Aggregates.sort(ascending("Position")), Aggregates.limit(5))).printResults()
        case 2 => collection.aggregate(Seq(Aggregates.group("$Artist", Accumulators.sum("totalStreams", "$Streams")), Aggregates.sort(descending("totalStreams")), Aggregates.limit(5))).printResults()
        case 3 => collection.aggregate(Seq(Aggregates.group("$Artist", Accumulators.avg("averageStreams", "$Streams")), Aggregates.sort(descending("averageStreams")), Aggregates.limit(5))).printResults()
        case 4 => println("Exiting...")
        case _ => println("Input 1-4 not detected. To view the top 5 Artist type 1. For the top 5 Artist by Total Streams type 2. For the top 5 Artist by Average Streams type 3. To EXIT type 4")
      }
    }
    case 2 => {
      println("To view top 5 songs type 1. For the top 5 Songs by Total Streams type 2. For the top 5 Songs by Average Streams type 3. To EXIT type 4.")
      var secondUserResponse: Int = readInt()

      secondUserResponse match {
        case 1 => collection.aggregate(Seq(project(fields(include("Position", "Track Name"), excludeId())), Aggregates.sort(ascending("Position")), Aggregates.limit(5))).printResults()
        case 2 => 
        case 3 =>
        case 4 => println("Exiting...")
        case _ => println("Input 1-4 not detected. To view the top 5 songs type 1. For the total amount of Streams for all songs type 2. For the top 5 Songs by Average Streams type 3. To EXIT type 4.")
      }
    }
      
    case _ => println("Please type 1 (Artists Information) or 2 (Songs Information)")
  }
  
  //collection.aggregate(project(fields(include("title", "author"), excludeId())).printResults()
  println("Waiting....") 
}
