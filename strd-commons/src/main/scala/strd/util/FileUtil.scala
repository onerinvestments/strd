package strd.util

import java.io.File

/**
 *
 * User: light
 * Date: 9/30/13
 * Time: 3:37 PM
 */
object FileUtil {
  def scanFiles(dir: File, condition: String => Boolean): Seq[File] = {
    val files = dir.listFiles()
    if (files == null) Nil else {
      val matches = files.filter(file => condition(file.getName))
      matches ++ files.filter(_.isDirectory).flatMap(d => scanFiles(d, condition))
    }
  }
}
