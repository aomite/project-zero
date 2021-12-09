//Helper
import example.Helpers._

//Scala
import org.mongodb.scala._
import scala.concurrent._
import io.StdIn._
import scala.io.Source
import scala.collection.SeqMap

//MongoDB Scala Driver
import org.mongodb.scala.model._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.bson.BsonObjectId
import org.bson.BsonValue
import org.bson.codecs.pojo.annotations.BsonId

object Main extends App {
  
  // Connect to Mongo server
  val client: MongoClient = MongoClient("mongodb://127.0.0.1:27017/")   // localhost: 27017

  // Reads CSV file and inserts it into a MongoDB database
  val cl: Runtime = Runtime.getRuntime(); 
  var runCommand: Process = null; 
  val command: String = "mongoimport --db test --collection proj-zero --type csv --headerline --drop C:/Users/amosc/Documents/Work/mgDemo/regional-us-daily-latest.csv"
  
  try {
    runCommand = cl.exec(command)
    println("Reading file into Database...")
    println("Successfully added file to specified Database")
  } catch {
      case e: Exception => {
        println("ERROR! Could not read CSV file")
    } 
  }

  val database: MongoDatabase = client.getDatabase("test")  

  val collection: MongoCollection[Document] = database.getCollection("proj-zero") 

  //Menu Interface  
  def mainMenu(userInput: Int) {
    try {
      if(userInput >= 5 || userInput <= 0){
        println("Incorrect Input. Please select a number between 1 and 4")
        println("[1] Information about Artists")
        println("[2] Information about Songs")
        println("[3] Add, update, or delete information")
        println("[4] EXIT program")
        var newUserResponse: Int = readInt()
        mainMenu(newUserResponse)
      } else if(userInput == 1){
          println("Artist Menu:")
          println("[1] Top 5 Artists by charting")
          println("[2] Top 5 Artists by total streams")
          println("[3] Top 5 Artists by average streams")
          println("[4] Main Menu")
          var newUserResponse: Int = readInt()
          artistMenu(newUserResponse)
      } else if(userInput == 2){
          println("Song Menu:")
          println("[1] Top 5 Songs by charting")
          println("[2] Total streams across all songs")
          println("[3] Song search")
          println("[4] Main Menu")
          var newUserResponse: Int = readInt()
          songMenu(newUserResponse)
      } else if(userInput == 3){
          println("Modification Menu:")
          println("[1] Add new document")
          println("[2] Update document")
          println("[3] Delete document")
          println("[4] Main Menu")
          var newUserResponse: Int = readInt()
          modificationMenu(newUserResponse)
      } else if(userInput == 4){
          println("Exiting program. Have a great day! :)")
          client.close()
      }
    } catch {
        case e: Exception => {
          println("Incorrect Input. Please select a number between 1 and 4")
          println("[1] Information about Artists")
          println("[2] Information about Songs")
          println("[3] Add, update, or delete information")
          println("[4] EXIT program")
          var newUserResponse: Int = readInt()
          mainMenu(newUserResponse)
        }
    }
  }
  
  def artistMenu(userInputTwo: Int) {
    try {
      if(userInputTwo >= 5 || userInputTwo <= 0){
        println("Incorrect input! Input 1-4 not detected.")
        getSecondResponseArtists()
      } else if(userInputTwo == 1){
          collection.aggregate(Seq(project(fields(include("Position", "Artist", "Streams"), excludeId())), Aggregates.sort(descending("Streams")), Aggregates.limit(5))).printResults()
          getSecondResponseArtists()
      } else if(userInputTwo == 2){
          collection.aggregate(Seq(Aggregates.group("$Artist", Accumulators.sum("totalStreams", "$Streams")), Aggregates.sort(descending("totalStreams")), Aggregates.limit(5))).printResults()
          getSecondResponseArtists()
      } else if(userInputTwo == 3){
          collection.aggregate(Seq(Aggregates.group("$Artist", Accumulators.avg("averageStreams", "$Streams")), Aggregates.sort(descending("averageStreams")), Aggregates.limit(5))).printResults()
          getSecondResponseArtists()
      } else if(userInputTwo == 4){
          println("Returning to Main Menu...")
          println("[1] Information about Artists")
          println("[2] Information about Songs")
          println("[3] Add, update, or delete information")
          println("[4] EXIT program")
          var newUserResponse: Int = readInt()
          mainMenu(newUserResponse)
      }
    } catch {
        case e: Exception => {

      }
  }
  }

  def songMenu(userInput: Int) {
    try{
      if(userInput >= 5 || userInput <= 0){
        println("Incorrect input! Input 1-4 not detected.")
        getSecondResponseSongs()
      } else if(userInput == 1){
          collection.aggregate(Seq(project(fields(include("Position", "Track Name", "Streams"), excludeId())), Aggregates.sort(descending("Streams")), Aggregates.limit(5))).printResults()
          getSecondResponseSongs()
      } else if(userInput == 2){
          collection.aggregate(Seq(Aggregates.group("$combinedTotalStreams", Accumulators.sum("combinedTotalStreams", "$Streams")), project(fields(excludeId())))).printResults()
          getSecondResponseSongs()
      } else if(userInput == 3){
          println("What Artist songs would you like to see?")
          var newUserResponse: String = readLine()
          collection.aggregate(Seq(project(fields(excludeId())), filter(equal("Artist", s"$newUserResponse")), Aggregates.sort(ascending("Position")))).printResults()
          getSecondResponseSongs()
      } else if(userInput == 4){
          println("Exiting to Main Menu")
          println("[1] Information about Artists")
          println("[2] Information about Songs")
          println("[3] Add, update, or delete information")
          println("[4] EXIT program")
          var newUserResponse: Int = readInt()
          mainMenu(newUserResponse)
      }
    } catch {
      case e: Exception => {

      }
    }
  }

  def modificationMenu(userInput: Int){
    try {
      if(userInput >=5 || userInput <= 0){
        println("Incorrect input! Input 1-4 not detected.")
      } else if(userInput == 1){
          addUserDocInfo()
      } else if(userInput == 2){
          updateUserDocInfo()
      } else if(userInput == 3){
          deleteUserDocInfo()
      } else if(userInput == 4){
          println("Exiting to Main Menu")
          println("[1] Information about Artists")
          println("[2] Information about Songs")
          println("[3] Add, update, or delete information")
          println("[4] EXIT program")          
          var newUserResponse: Int = readInt()
          mainMenu(newUserResponse)
      }
    } catch {
      case e: Exception => {
        println("An error occurred. Please try again.")
        getSecondResponseModification()
      }
    }
  }

  // Methods for userSelectionSecondary- Artists && Songs
  def getSecondResponseArtists() {
    println("Artist Menu:")
    println("[1] Top 5 Artists by charting")
    println("[2] Top 5 Artists by total streams")
    println("[3] Top 5 Artists by average streams")
    println("[4] Main Menu")    
    var secondUserResponse: Int = readInt()
    artistMenu(secondUserResponse)
  }

  def getSecondResponseSongs() {
    println("Song Menu:")
    println("[1] Top 5 Songs by charting")
    println("[2] Total streams across all songs")
    println("[3] Song search")
    println("[4] Main Menu")    
    var secondUserResponse: Int = readInt()
    songMenu(secondUserResponse)
  }

  def getSecondResponseModification() {
    println("Modification Menu:")
    println("[1] Add new document")
    println("[2] Update document")
    println("[3] Delete document")
    println("[4] Main Menu")    
    var newUserResponse: Int = readInt()
    modificationMenu(newUserResponse)
  }
  
  
  //Add
  def addUserDocInfo(){
    println("Creating new document...")
    println("Please enter the song title...")
    var songTitle: String = readLine()

    println("Please enter the artist name...")
    var artistName: String = readLine()

    println("Please enter the number of streams...")
    var streamCount: Int = readInt()

    println("Creating new document...")
    
    var userDocument: Document = Document (
      "Track Name"-> songTitle,
      "Artist" -> artistName,
      "Streams" -> streamCount
    )
    
    collection.insertOne(userDocument).results()
    println("Document successfully added.")
    collection.aggregate(Seq(filter(equal("Artist", s"$artistName")))).printResults()

    println("Exiting to Modification Menu...")

    println("Modification Menu:")
    println("[1] Add new document")
    println("[2] Update document")
    println("[3] Delete document")
    println("[4] Main Menu")    
    var newUserResponse: Int = readInt()
    modificationMenu(newUserResponse)
  }

  //Update
  def updateUserDocInfo(){
    println("What is the name of the Artist on the document you wish to update?")
    var artistName: String = readLine()

    println("What is the name of the Song on the document you wish to update?")
    var songTitle: String = readLine()
    
    println("Document found. Which property would you like to update:")
    println("[1] Artist Name")
    println("[2] Song Title")
    println("[3] CANCEL")
    var artistOrSong = readInt()

    if(artistOrSong >= 4 || artistOrSong <= 0){
        println("Incorrect Input. Exiting to Modification Menu...")
        getSecondResponseModification()
    } else if(artistOrSong == 1){
        println("What is the Artist's new name?")
        var newArtist: String = readLine()
        collection.updateOne(and(equal("Artist", s"$artistName"), equal("Track Name", s"$songTitle")), set("Artist", s"$newArtist")).printResults()
        collection.aggregate(Seq(filter(equal("Artist", s"$newArtist")))).printResults()
        println("Artist update complete. Exiting to Modification Menu...")

        getSecondResponseModification()
    } else if(artistOrSong == 2){ 
        println("What is the new Song's title?")
        var newSong: String = readLine()
        collection.updateOne(and(equal("Artist", s"$artistName"), equal("Track Name", s"$songTitle")), set("Track Name", s"$newSong")).printResults()
        collection.aggregate(Seq(filter(equal("Track Name", s"$newSong")))).printResults()
        println("Song update complete. Exiting to Modification Menu...")

        getSecondResponseModification()
    } else if(artistOrSong == 3){
        println("Exiting to Modification Menu...")
        getSecondResponseModification()
    }
  }

  //Delete
  def deleteUserDocInfo(){
    println("What is the name of the Artist on the document you wish to delete?")
    var artistName: String = readLine()
   
    collection.deleteOne(equal("Artist", s"$artistName")).results()
    println("Document deleted. Exiting to Modification Menu")
    collection.aggregate(Seq(filter(equal("Artist", s"$artistName")))).printResults()

    println("Modification Menu:")
    println("[1] Add new document")
    println("[2] Update document")
    println("[3] Delete document")
    println("[4] Main Menu")
    var newUserResponse: Int = readInt()
    modificationMenu(newUserResponse)
  }



  //UX Start: Main Menu
  println("Welcome! This program uses information from Spotify's top 200 US chart.")
  println("[1] Information about Artists")
  println("[2] Information about Songs")
  println("[3] Add, update, or delete information")
  println("[4] EXIT program")
  var userResponse: Int = readInt(); 
  mainMenu(userResponse)
  println("Successfully closed")
}
