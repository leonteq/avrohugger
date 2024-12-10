package avrohugger
package input
package parsers

import java.io.File
import scala.util.matching.Regex.Match

object IdlImportParser {

  def stripComments(fileContents: String): String = {
    val multiLinePattern  = """/\*.*\*/""".r
    val singleLinePattern = """//.*$""".r
    val noSingleLines     = singleLinePattern.replaceAllIn(fileContents, "")
    val commentFree       = multiLinePattern.replaceAllIn(noSingleLines, "")
    commentFree
  }

  def getImportedFiles(infile: File, classLoader: ClassLoader): Set[File] = {
    def readFile(file: File): String = {
      var count = 0
      val maxTries = 3
      try {
        count += 1
        val file = scala.io.Source.fromFile(infile)
        val fileContents: String = stripComments(file.mkString)
        file.close
        // if file is empty, try again, it was there when we read idl
        if (fileContents.isEmpty && (count < maxTries)) readFile(infile)
        else fileContents
      } catch { // if file is not found, try again, it was there when we read idl
        case e: java.io.FileNotFoundException =>
          if (count < maxTries) readFile(infile)
          else sys.error("File to found: " + infile)
      }
    }
    val path            = infile.getParent + "/"
    val contents        = readFile(infile)
    val avdlPattern     = """import[ \t]+idl[ \t]+"([^"]*\.avdl)"[ \t]*;""".r
    val avprPattern     = """import[ \t]+protocol[ \t]+"([^"]*\.avpr)"[ \t]*;""".r
    val avscPattern     = """import[ \t]+schema[ \t]+"([^"]*\.avsc)"[ \t]*;""".r
    val idlMatches      = avdlPattern.findAllIn(contents).matchData.toSet
    val protocolMatches = avprPattern.findAllIn(contents).matchData.toSet
    val schemaMatches   = avscPattern.findAllIn(contents).matchData.toSet
    val importMatches   = idlMatches ++ protocolMatches ++ schemaMatches

    val (localImports, nonLocalMatches): (Set[File], Set[Match]) =
      importMatches.foldLeft((Set.empty[File], Set.empty[Match])) { case ((ai, am), m) =>
        val f = new File(path + m.group(1))
        if (f.exists) (ai + f, am)
        else (ai, am + m)
      }

    val classpathImports: Set[File] = nonLocalMatches
      .map(m => Option(classLoader.getResource(m.group(1))).map(resource => new File(resource.getFile)))
      .flatMap(_.toList)
      .filter(file => file.exists)

    classpathImports ++ localImports
  }

}
