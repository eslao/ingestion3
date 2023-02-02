package dpla.eleanor.harvesters.opds2

import dpla.eleanor.HarvestStatics
import dpla.eleanor.Schemata.{HarvestData, MetadataType, Payload, SourceUri}
import dpla.eleanor.harvesters.Retry
import dpla.eleanor.harvesters.opds2.FeedParser.parsePages
import dpla.ingestion3.model.formats
import okhttp3.{Authenticator, Credentials, OkHttpClient, Request, Response, Route}
import org.apache.spark.sql.{Dataset, SparkSession}

import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JString, JValue}

import java.io.{File, FileWriter, PrintWriter}
import java.net.URL
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import java.sql.Timestamp
import scala.io.Source

class Opds2Harvester(timestamp: Timestamp,
                     source: SourceUri,
                     metadataType: MetadataType)
  extends Serializable {

  def execute(spark: SparkSession, feedUrl: String): Dataset[HarvestData] = {

    val feedSource = Source.fromURL(feedUrl)
    val feedString = feedSource.mkString
    feedSource.close()

    val json = JsonMethods.parse(feedString)
    val navFeedUrls = for (
      JString(url) <- json \ "navigation" \ "href"
    ) yield url

    val feedUrls =
      if (navFeedUrls.nonEmpty) navFeedUrls
      else Seq(feedUrl)

    val harvestStatics: HarvestStatics = HarvestStatics(
      sourceUri = source.uri,
      timestamp = timestamp,
      metadataType = metadataType
    )

    import spark.implicits._

    val results = for (url <- feedUrls) yield {
      val tmpDir = Files.createTempDirectory("opds2-")
      FeedReader.harvest(url, tmpDir.toString, None, None) match {
        case Success(_) =>
          // return data path
          tmpDir.toString

        case Failure(ex) =>
          throw new Exception("Harvest failure.", ex)
      }
    }

    val rawData = spark
      .read
      .text(results: _*)
      .as[String]

    rawData.map(line => {
      println(line)
      val json = JsonMethods.parse(line)
      val id = (json \ "metadata" \ "identifier").extract[String]
      val payloads = (
        (for (JString(link) <- json \ "links" \ "href") yield link)
          :::
          (for (JString(link) <- json \ "images" \ "href") yield link)
        ).map(x => Payload(url = x))

      HarvestData(
        sourceUri = harvestStatics.sourceUri,
        id = id,
        timestamp = timestamp,
        metadataType = MetadataType.Opds2,
        metadata = line.getBytes("utf-8"),
        payloads = payloads
      )
    })


  }
}


object FeedReader {

  def harvest(
               urlStr: String,
               outDirStr: String,
               username: Option[String],
               password: Option[String]
             ): Try[Unit] = {

    def publicationCallback(json: JValue)(implicit out: PrintWriter): Unit =
      out.println(JsonMethods.compact(json))

    implicit val outDir: File = new File(outDirStr)

    if (!outDir.exists()) outDir.mkdirs()

    implicit val jsonlOut: PrintWriter = new PrintWriter(
      new FileWriter(
        new File(outDir, "harvest.jsonl")
      )
    )

    implicit val client: OkHttpClient =
      HttpClientBuilder.getClient(username, password)

    val result = Try(
      parsePages(
        new URL(urlStr), 1, publicationCallback
      )
    )
    val _ = Try(jsonlOut.close())
    result
  }
}

object HttpClientBuilder {

  private val CONNECT_TIMEOUT = 10L
  private val WRITE_TIMEOUT = 10L
  private val READ_TIMEOUT = 60L

  def getClient(usernameOption: Option[String], passwordOption: Option[String]): OkHttpClient = {

    val builder = new OkHttpClient.Builder()
      .connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(WRITE_TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(READ_TIMEOUT, TimeUnit.SECONDS)

    (usernameOption, passwordOption) match {
      case (Some(user), Some(pass)) =>
        builder.authenticator(new Authenticator() {
          override def authenticate(route: Route, response: Response): Request = {
            response
              .request()
              .newBuilder()
              .header("Authorization", Credentials.basic(user, pass))
              .build()
          }
        })
      case _ =>
    }

    builder.build()
  }
}


object FeedParser extends Retry {

  private val RETRY_COUNT = 5

  @tailrec
  def parsePages(
                  location: URL,
                  pageNum: Int,
                  publicationCallback: JValue => Unit
                )
                (implicit client: OkHttpClient): Unit = {

    // get page string
    val pageStringTry = retry(RETRY_COUNT) {
      println(s"Requesting ${location.toString}")
      val request = new Request.Builder()
        .url(location)
        .build()
      val response = client.newCall(request).execute()
      response.body().string()
    }

    val pageString = pageStringTry.get

    val json = JsonMethods.parse(pageString)

    // write out publications
    json \ "publications" match {
      case JArray(children) => children.foreach(publicationCallback)
      case _ => throw new Exception("Didn't find publications")
    }

    // recurse on "next" link if available
    getNextLink(json) match {
      case Some(next) =>
        parsePages(
          new URL(next),
          pageNum + 1,
          publicationCallback
        )
      case _ => // falls through
    }
  }

  private def getNextLink(json: JValue): Option[String] = {
    val nextLinks = for {
      JArray(links) <- json \ "links"
      link <- links
      if link \ "type" == JString("application/opds+json")
      if link \ "rel" == JString("next")
      JString(href) <- link \ "href"
    } yield href
    nextLinks.headOption
  }
}




