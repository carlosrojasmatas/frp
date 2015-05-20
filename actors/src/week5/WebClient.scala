package week5

import com.ning.http.client.AsyncHttpClient
import scala.concurrent.Future
import java.util.concurrent.Executor
import scala.concurrent.Promise
import org.jsoup.Jsoup
import scala.collection.JavaConverters._

object WebClient {

  case class BadStatus(code: Int) extends Throwable {
    require(code >= 200 && code < 600)
  }
   
   
  private val client = new AsyncHttpClient
  
  def get(url:String)(implicit exec:Executor) : Future[String] = {
    
    val f = client.prepareGet(url).execute()
    val p = Promise[String]()
    f.addListener(new Runnable {
      def run = {
        val response = f.get
        if(response.getStatusCode < 400)
          p.success(response.getResponseBodyExcerpt(131072))
        else p.failure(BadStatus(response.getStatusCode))
      }
    }, exec)
    p.future
  }
  
  def findLinks(body:String):Iterator[String] = {
    val doc = Jsoup.parse(body)
    val links = doc.select("a[href]")
    for{
      link <- links.iterator().asScala
    }yield link.absUrl("href")
  }
  
  def shutdown() {
    client.close()
  }
}