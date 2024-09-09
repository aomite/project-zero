import org.mongodb.scala._
import scala.concurrent._
import org.mongodb.scala.model._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import io.StdIn._
import scala.io.Source  

import example.Helpers._
import org.bson.BsonValue
import org.bson.codecs.pojo.annotations.BsonId
import scala.collection.SeqMap

 import org.mongodb.scala.model.UpdateOptions
 import org.mongodb.scala.bson.BsonObjectId

object Main extends App {
  
  val client: MongoClient = MongoClient("mongodb://127.0.0.1:27017/")   // localhost: 27017

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

  val database: MongoDatabase = client.getDatabase("test")    // use test

  val collection: MongoCollection[Document] = database.getCollection("mgPractice")    // db.mgPractice
  
  def userSelectionMain(userInput: Int) {
    if(userInput >= 5 || userInput <= 0){
      println("Error! Incorrect Input. To view information about Artists type 1. For information regarding Songs type 2. To add, update, or delete information type 3. To EXIT PROGRAM type 4.")
      var newUserResponse: Int = readInt()
      userSelectionMain(newUserResponse)
    } else if(userInput == 1){
        println("Artist Menu: To view the top 5 Artist type 1. For the top 5 Artist by Total Streams type 2. For the top 5 Artist by Average Streams type 3. To RETURN to Main Menu type 4.")
        var newUserResponse: Int = readInt()
        userSelectionSecondaryArtists(newUserResponse)
    } else if(userInput == 2){
        println("Song Menu: To view the top 5 songs type 1. For the total amount of Streams across all songs type 2. To search Songs in the top 200 US chart by artist type 3. To RETURN to Main Menu type type 4.")
        var newUserResponse: Int = readInt()
        userSelectionSecondarySongs(newUserResponse)
    } else if(userInput == 3){
        println("Modification Menu: To add a new document type 1. To update a document type 2. To delete a document type 3. To RETURN to Main Menu type 4.")
        var newUserResponse: Int = readInt()
        userSelectionSecondaryModification(newUserResponse)
    } else if(userInput == 4){
        println("Exiting...")
        client.close()
    }
  }
  
  def userSelectionSecondaryArtists(userInputTwo: Int) {
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
        println("Exiting to Main Menu")
        println("To view information about Artists type 1. For information regarding Songs type 2. To add, update, or delete information type 3. To EXIT PROGRAM type 4.")
        var newUserResponse: Int = readInt()
        userSelectionMain(newUserResponse)
    }
  }

  def userSelectionSecondarySongs(userInput: Int) {
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
        println("To view information about Artists type 1. For information regarding Songs type 2. To add, update, or delete information type 3. To EXIT PROGRAM type 4.")
        var newUserResponse: Int = readInt()
        userSelectionMain(newUserResponse)
    }
  }

  def userSelectionSecondaryModification(userInput: Int){
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
        println("To view information about Artists type 1. For information regarding Songs type 2. To add, update, or delete information type 3. To EXIT PROGRAM type 4.")
        var newUserResponse: Int = readInt()
        userSelectionMain(newUserResponse)
    }
  }

  def getSecondResponseArtists() {
    println("To view the top 5 Artist type 1. For the top 5 Artist by Total Streams type 2. For the top 5 Artist by Average Streams type 3. To RETURN to Main Menu type 4")
    var secondUserResponse: Int = readInt()
    userSelectionSecondaryArtists(secondUserResponse)
  }

  def getSecondResponseSongs() {
    println("To view the top 5 songs type 1. For the total amount of Streams across all songs type 2. To search Songs in the top 200 US chart by Artist type 3. To RETURN to Main Menu type 4.")
    var secondUserResponse: Int = readInt()
    userSelectionSecondarySongs(secondUserResponse)
  }

  def getSecondResponseModification() {
    println("Modification Menu: To add a new document type 1. To update a document type 2. To delete a document type 3. To RETURN to Main Menu type 4.")
    var newUserResponse: Int = readInt()
    userSelectionSecondaryModification(newUserResponse)
  }
  
  
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

    println("Modification Menu: To add a new document type 1. To update a document type 2. To delete a document type 3. To RETURN to Main Menu type 4.")
    var newUserResponse: Int = readInt()
    userSelectionSecondaryModification(newUserResponse)
  }

  def updateUserDocInfo(){
    println("What is the name of the Artist on the document you wish to update?")
    var artistName: String = readLine()

    println("What is the name of the Song on the document you wish to update?")
    var songTitle: String = readLine()
    
    println("Document found. Which property would you like to update: Artist Name (Type 1) or Song Title (Type 2)? To CANCEL type 3.")
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

  def deleteUserDocInfo(){
    println("What is the name of the Artist on the document you wish to delete?")
    var artistName: String = readLine()
   
    collection.deleteOne(equal("Artist", s"$artistName")).results()
    println("Document deleted. Exiting to Modification Menu")
    collection.aggregate(Seq(filter(equal("Artist", s"$artistName")))).printResults()

    println("Modification Menu: To add a new document type 1. To update a document type 2. To delete a document type 3. To RETURN to Main Menu type 4.")
    var newUserResponse: Int = readInt()
    userSelectionSecondaryModification(newUserResponse)
  }

  println("Welcome! This program uses information from Spotify's top 200 US chart.")
  println("To view information about Artists type 1. For information regarding Songs type 2. To add, update, or delete information type 3. To EXIT PROGRAM type 4.")
  var userResponse: Int = readInt(); 
  userSelectionMain(userResponse)

  println("Program Closed")
}
