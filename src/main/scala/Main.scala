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
  println("To view information about Artists type 1. For information regarding Songs type 2. To EXIT type 3.")

  var userResponse: Int = readInt(); 

  def userSelectionMain(userInput: Int) {
    if(userInput >= 4 || userInput <= 0){
      println("Error! Incorrect Input. To view information about Artists type 1. For information regarding Songs type 2. To EXIT type 3.")
      var newUserResponse: Int = readInt()
      userSelectionMain(newUserResponse)
    } else if(userInput == 1){
        println("Artist Menu: To view the top 5 Artist type 1. For the top 5 Artist by Total Streams type 2. For the top 5 Artist by Average Streams type 3. To EXIT type 4.")
        var newUserResponse: Int = readInt()
        userSelectionSecondaryArtists(newUserResponse)
    } else if(userInput == 2){
        println("Song Menu: To view the top 5 songs type 1. For the total amount of Streams across all songs type 2. For the top 5 Songs by Average Streams type 3. To EXIT type 4.")
        var newUserResponse: Int = readInt()
        userSelectionSecondarySongs(newUserResponse)
    } else if(userInput == 3){
        println("Exiting...")
        client.close()
    }
  }

  userSelectionMain(userResponse)
  
  def userSelectionSecondaryArtists(userInputTwo: Int) {
    if(userInputTwo >= 5 || userInputTwo <= 0){
      println("Incorrect input! Input 1-4 not detected.")
      getSecondResponseArtists()
    } else if(userInputTwo == 1){
        collection.aggregate(Seq(project(fields(include("Position", "Track Name"), excludeId())), Aggregates.sort(ascending("Position")), Aggregates.limit(5))).printResults()
        getSecondResponseArtists()
    } else if(userInputTwo == 2){
        collection.aggregate(Seq(Aggregates.group("$Artist", Accumulators.sum("totalStreams", "$Streams")), Aggregates.sort(descending("totalStreams")), Aggregates.limit(5))).printResults()
        getSecondResponseArtists()
    } else if(userInputTwo == 3){
        collection.aggregate(Seq(Aggregates.group("$Artist", Accumulators.avg("averageStreams", "$Streams")), Aggregates.sort(descending("averageStreams")), Aggregates.limit(5))).printResults()
        getSecondResponseArtists()
    } else if(userInputTwo == 4){
        println("Exiting to Main Menu")
        println("To view information about Artists type 1. For information regarding Songs type 2. To EXIT type 3.")
        var newUserResponse: Int = readInt()
        userSelectionMain(newUserResponse)
    }
  }

  def userSelectionSecondarySongs(userInputTwo: Int) {
    if(userInputTwo >= 5 || userInputTwo <= 0){
      println("Incorrect input! Input 1-4 not detected.")
      getSecondResponseSongs()
    } else if(userInputTwo == 1){
        collection.aggregate(Seq(project(fields(include("Position", "Artist"), excludeId())), Aggregates.sort(ascending("Position")), Aggregates.limit(5))).printResults()
        getSecondResponseSongs()
    } else if(userInputTwo == 2){
        collection.aggregate(Seq(Aggregates.group("$combinedTotalStreams", Accumulators.sum("combinedTotalStreams", "$Streams")), project(fields(excludeId())))).printResults()
        getSecondResponseSongs()
    } else if(userInputTwo == 3){
        println("Coming soon")
        getSecondResponseSongs()
    } else if(userInputTwo == 4){
        println("Exiting to Main Menu")
        println("To view information about Artists type 1. For information regarding Songs type 2. To EXIT type 3.")
        var newUserResponse: Int = readInt()
        userSelectionMain(newUserResponse)
    }
  }

  def getSecondResponseArtists() {
    println("To view the top 5 Artist type 1. For the top 5 Artist by Total Streams type 2. For the top 5 Artist by Average Streams type 3. To EXIT to Main Menu type 4")
    var secondUserResponse: Int = readInt()
    userSelectionSecondaryArtists(secondUserResponse)
  }

  def getSecondResponseSongs() {
    println("To view the top 5 songs type 1. For the total amount of Streams across all songs type 2. For the top 5 Songs by Average Streams type 3. To EXIT to Main Menu type 4.")
    var secondUserResponse: Int = readInt()
    userSelectionSecondarySongs(secondUserResponse)
  }

  println("Program Closed") 
}
