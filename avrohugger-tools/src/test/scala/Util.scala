import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.IOException

object Util {

  def readFile(fileName: String, maxTries: Int = 3): String = {
    def readFile0(count: Int): String =
      try { // if file is empty, try again, it should be there
        val contents: String = scala.io.Source.fromFile(fileName).mkString
        if (contents.isEmpty && (count < maxTries)) readFile0(count + 1)
        else contents
      } catch { // if file is not found, try again, it should be there
        case e: Throwable =>
          if (count < maxTries) readFile0(count + 1)
          else sys.error("File not found: " + fileName)
      }
    readFile0(0)
  }

}
