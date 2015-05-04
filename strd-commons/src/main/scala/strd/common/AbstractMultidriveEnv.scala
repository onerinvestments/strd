package strd.common

import java.io.{FileInputStream, FileFilter, FileOutputStream, File}
import com.twitter.ostrich.stats.Stats
import org.slf4j.LoggerFactory
import org.apache.commons.io.FileUtils
import io.netty.util.CharsetUtil

/**
 *
 * User: light
 * Date: 23/04/14
 * Time: 17:42
 */

abstract class AbstractMultidriveEnv[X <: BaseDriveEntry] (val home : File) {
  val log = LoggerFactory.getLogger(getClass)
  log.info(s"Find drives under: ${home.getPath}")

  def baseName : String

  def assignDrive( id : Int, name : String, base : File ) : X

  val allDrives =
    Option( home.listFiles( new FileFilter(){
      def accept(pathname: File) = {
        pathname.isDirectory && pathname.getName.startsWith("disk")
      }
    })).map(_.toSeq).getOrElse(Nil)

  if (allDrives.isEmpty) {
    throw new RuntimeException("No drives found under: " + home)
  }

  def getDriveForId(id : Int) = drvMap.get(id)

  val totalDrivesCount = {
    val descrFile = new File( home, ".multidrives")

    readIntFromFile( descrFile).getOrElse {
      val drvCount = allDrives.size
      writeIntToFile(descrFile, drvCount )
      log.info(".multidrives file initiated with: " + drvCount + " drives")
      drvCount
    }
  }

  val regexp = """disk(\d+)""".r

  val drvMap = collection.mutable.Map.empty[Int, X]

  val drives = allDrives.map(ff => {
    val f = new File(ff, baseName)
    f.mkdirs()

    if (!f.exists()) {
      throw new RuntimeException("can not create directory: " + f.getPath)
    }

    val id = {
      val idFile = new File( f, ".drive_id")
      readIntFromFile(idFile).getOrElse{
        val idd = ff.getName match {
          case regexp(num) => num.toInt
        }

        if (idd > totalDrivesCount) {
          throw new IllegalStateException(s"drive id $idd > $totalDrivesCount   path:${ff.getPath}")
        }

        if (drvMap.contains(idd)) {
          throw new IllegalStateException(s"drive ${ff.getPath}  has id $idd, but we already have this id under drive :${drvMap(idd).base.getPath}")
        }

        writeIntToFile(idFile, idd)
        log.debug(s"Assign Drive ID: $idd to ${ff.getPath}")
        idd
      }
    }

    if (id > totalDrivesCount) {
      throw new IllegalStateException(s"drive id $id > $totalDrivesCount")
    }

    if (drvMap.contains(id)) {
      throw new IllegalStateException(s"drive ${ff.getPath}  has id $id, but we already have this id under drive :${drvMap(id).base.getPath}")
    }

    val de = assignDrive(id, ff.getName, f)
    drvMap.put(id, de)

    if (!de.touchAlive) {
      throw new RuntimeException("Drive is readonly or damaged: " + f.getAbsolutePath)
    }

    FileUtils.cleanDirectory(de.tmp)

    Stats.addGauge("drives/" + de.name + "/freeSpace") {
      de.freeGigabytes
    }

    de
  })

  if (totalDrivesCount != drives.size) {
    log.error(s"Drives count changed -> you must update .multidrives file ( available: ${drives.size}, memoized $totalDrivesCount )")
  }

  drives.foreach(x => {
    println( s"Drive (${x.id} => ${x.name}}): ${x.base.getPath} has: ${x.freeGigabytes} GB, slow:" + x.isSlowDrive)
  })

  def totalFreeSpace =  drives.map(_.freeSpace).sum
  def avgFreeSpacePerDrive = totalFreeSpace / math.max( drives.size, 1 )

  def getNextFreeDrive = {
    val toRandom = drives.filter(_.touchAlive).filter(x => ! x.isSlowDrive).groupBy(_.freeGigabytes).toSeq.sortBy(_._1).last._2
    toRandom( (toRandom.size * math.random).toInt )
  }

  def writeIntToFile( f:File, value : Int) {
    val bytes = value.toString.getBytes(CharsetUtil.UTF_8)
    val fos = new FileOutputStream(f)
    fos.write(bytes)
    fos.close()
  }

  def readIntFromFile(f : File) : Option[Int] = {
    try {
      if ( f.exists() ) {
        val fis = new FileInputStream(f)
        val bytes = new Array[Byte]( fis.available() )
        fis.read(bytes)
        Some( new String(bytes, CharsetUtil.UTF_8).trim.toInt )
      } else {
        None
      }
    } catch {
      case x: Exception => None
    }
  }

  val drivesArray = {
    val array = new Array[Option[X]](totalDrivesCount)
    0.until(totalDrivesCount).foreach{ i => array(i) = None }
    drvMap.values.foreach( d => array(d.id - 1) = Some(d))
    array
  }

}

abstract class BaseDriveEntry( val name : String,
                               val id : Int,
                               val base : File ) {
  val tmp  = new File(base, "tmp")
  val slowStamp = new File(base, ".slow" )

  if ( (! tmp.exists() && ! tmp.mkdirs()) || ( tmp.exists() && ! tmp.canWrite ) ) {
    throw new IllegalStateException("can not created tmp: " + tmp.getPath + "  writable:" + tmp.canWrite + " exists: " + tmp.exists() )
  }

  def isSlowDrive = slowStamp.exists()

  def touchAlive = {
    try {
      val touch = new File(tmp, ".touch." + System.currentTimeMillis())
      touch.createNewFile()
      val fos = new FileOutputStream(touch, false)
      fos.write(1)
      fos.close()

      touch.delete()

      true
    } catch {
      case x:Exception =>
        Stats.setLabel("drives/" + name +"/alive", "BROKEN!")
        LoggerFactory.getLogger(getClass).warn("Drive is not alive: " + tmp.getAbsolutePath, x)
        false
    }
  }

  def freeSpace = tmp.getFreeSpace
  def freeGigabytes = (freeSpace / 1024d / 1024d / 1024d).toInt

}