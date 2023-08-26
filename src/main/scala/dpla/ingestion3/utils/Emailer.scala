package dpla.ingestion3.utils

import com.amazonaws.services.simpleemail._
import com.amazonaws.services.simpleemail.model._
import dpla.ingestion3.confs.i3Conf
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.{ExcludeFileFilter, ZipParameters}

import java.io.{ByteArrayOutputStream, File}
import java.nio
import java.nio.file.Files
import java.util.Properties
import javax.activation.DataHandler
import javax.mail.Message.RecipientType
import javax.mail._
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.util.ByteArrayDataSource
import scala.io.{BufferedSource, Source}

object Emailer {
  private val sender = "DPLA Bot<tech@dp.la>"

  private val prefix =
    s"""
      |This is an automated email summarizing the DPLA ingest. Please see attached ZIP file
      |for record level information about errors and warnings.
      |
      |If you have questions please contact us at <a href="mailto:tech@dp.la">tech@dp.la</a>
      |
      |- <a href="https://github.com/dpla/ingestion3/">Ingestion documentation</a>
      |- <a href="about:blank">Wikimedia project</a>
      |- <a href="about:blank">DPLA news</a>
      |""".stripMargin.split("\n")

  private lazy val suffix =
    """
      |
      |
      |Bleep bloop.
      |
      |-----------------  END  -----------------
      |
      |""".stripMargin.split("\n")
  // Only include the *.csv files in the zipped export
  lazy val excludeFileFilter: ExcludeFileFilter = new ExcludeFileFilter {
    override def isExcluded(file: File): Boolean = {
      file.isFile & !file.getName.endsWith("csv")
    }
  }
  lazy val zipParameters: ZipParameters = new ZipParameters()
  zipParameters.setExcludeFileFilter(excludeFileFilter)

  def emailSummary(mapOutput: String, partner: String, i3conf: i3Conf): Unit = {
    val emails = i3conf.email.getOrElse("tech@dp.la").split(',')

    val _summary = s"${mapOutput}/_SUMMARY"
    val zipped_logs = s"${mapOutput}/_LOGS/logs.zip" // FIXME provider-date-mapping-logs.zip

    val body = emailBody(_summary)
    val zip_file = zip(zipped_logs, mapOutput)
    send(
      recipients = emails,
      subject = s"DPLA Ingest Summary for $partner", // FIXME Add current month in subject
      text = body,
      attachments = Seq(zip_file)
    )
  }

  private def emailBody(summaryFile: String): String = {
    // READ in _SUMMARY file
    val source: BufferedSource = Source.fromFile(summaryFile)
    val lines = try {
      source.getLines().toList
    } finally {
      source.close
    }
    // Create body of email by wrapping text in <pre> tags and dropping the last five line (which reference local
    // log files)
    List.concat(List("<pre>"),
      prefix,
      lines.dropRight(5),
      suffix,
      List("</pre>")).mkString("\n")
  }

  private def zip(zipped_logs: String, mapOutput: String): File = {
    // Build the zip file contents
    val zip_out = new ZipFile(zipped_logs)
    val warnings = new File(s"${mapOutput}/_LOGS/warnings/")
    val errors = new File(s"${mapOutput}/_LOGS/errors/")

    if(warnings.exists())
      zip_out.addFolder(warnings, zipParameters)
    if(errors.exists())
      zip_out.addFolder(errors, zipParameters)
    zip_out.close()

    new File(zipped_logs)
  }

  // Body must be plain text - HTML markup would require a `withHtml` call, not a `withText` call.
  private def send(recipients: Seq[String], subject: String, text: String, attachments: Seq[File]): Unit = {
    // Message builder to add attachments
    // https://docs.aws.amazon.com/ses/latest/dg/example_ses_SendEmail_section.html

    try {
      val client = AmazonSimpleEmailServiceClientBuilder.defaultClient()
      val file = attachments.head

      val session = Session.getDefaultInstance(new Properties())
      val message = new MimeMessage(session)

      message.setSubject(subject)

      val sendTo: Array[Address] = recipients.toArray.map(new InternetAddress(_))
      val replyTo: Array[Address] = Array(new InternetAddress(sender))
      val sendFrom: Address = new InternetAddress(sender)
      val sendCc: Array[Address] = Array[Address](
        new InternetAddress("tech@dp.la")
//        TODO Who else should be cc'd on these?
//        , new InternetAddress("shanee@dp.la")
      )

      message.setFrom(sendFrom)
      message.setReplyTo(replyTo)
      message.setRecipients(RecipientType.TO, sendTo)
      message.setRecipients(RecipientType.CC, sendCc)

      val messageBody = new MimeMultipart("alternative")
      val wrap = new MimeBodyPart()

      val htmlPart = new MimeBodyPart()
      htmlPart.setText(text, "utf-8", "html")
      messageBody.addBodyPart(htmlPart)

      // TODO text part
      wrap.setContent(messageBody)

      val msg = new MimeMultipart("mixed")
      message.setContent(msg)
      msg.addBodyPart(wrap)

      // Define the attachment.
      val fileBytes = Files.readAllBytes(file.toPath)
      val att = new MimeBodyPart()
      val fds = new ByteArrayDataSource(fileBytes, "application/zip;")
      att.setDataHandler(new DataHandler(fds))
      val reportName: String = "log_file.zip"
      att.setFileName(reportName)
      msg.addBodyPart(att)

      val bos = new ByteArrayOutputStream()

      message.writeTo(bos)
      val bb = nio.ByteBuffer.wrap(bos.toByteArray)
      val rawMessage = new RawMessage().withData(bb)

      val sendRawEmailRequest = new SendRawEmailRequest().withRawMessage(rawMessage)
      client.sendRawEmail(sendRawEmailRequest)

      System.out.println(s"Email sent to ${recipients.mkString(", ")}")
    } catch {
      case ex: Exception =>
        throw ex
    }
  }
}