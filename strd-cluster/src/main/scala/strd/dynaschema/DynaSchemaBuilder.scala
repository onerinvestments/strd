package strd.dynaschema

import java.io.FileOutputStream
import java.util.jar.{JarEntry, JarOutputStream, Attributes, Manifest}
import org.apache.commons.lang.StringUtils


/**
 *
 * User: light
 * Date: 16/04/14
 * Time: 16:00
 */

case class ResEntry(path :String, data : Array[Byte])

class DynaSchemaBuilder {
  var jarExtra : Seq[ResEntry] = Nil

  def appendJarResource(path: String, bytes: Array[Byte]) = {
    jarExtra = jarExtra :+ ResEntry(path, bytes)
    this
  }


  var classes : Seq[Class[_]] = Nil
  var protoClasses : Seq[Class[_]] = Nil

  var includePackages : Seq[String] = Nil
  var excludePackages : Seq[String] = Nil


  def appendClass( cl : Class[_] ) = {

    classes :+= cl
    this
  }

  def appendProtoClass( cl : Class[_]) = {
    protoClasses :+= cl
    this
  }

  def includePackage( pack : String ) = {
    includePackages :+= pack
    this
  }

  def excludePackage( pack : String ) = {
    excludePackages :+= pack
    this
  }

  def assembly() = {
    new ClassesProcessor(this).build()
  }

}

class ClassesProcessor( db : DynaSchemaBuilder ) {

  def build() :ClassesWithDependencies = {

    val classes = db.classes.toSet[Class[_]].flatMap { cl =>
      ClassWorksHelper.getAllDependencies(cl)
    }
    val protoClasses = db.protoClasses.flatMap { cl =>
      cl +: ClassWorksHelper.asmFetchAllInnerClasses(cl)
    }.toSet

    val allClasses = (classes ++ protoClasses)
      .filter { cl =>
        db.includePackages.isEmpty || db.includePackages.exists(pack => cl.getName.startsWith(pack))
      }.filter { cl =>
        db.excludePackages.isEmpty || !db.excludePackages.exists(pack => cl.getName.startsWith(pack))
      }

    ClassesWithDependencies( allClasses, db )
  }

}

case class ClassesWithDependencies( allClasses : Set[Class[_]], db : DynaSchemaBuilder ) {
  def createJar( schemaType : String,
                 schemaVersion : String ) = {

    val jarPath = "/tmp/"+schemaType + "_" + schemaVersion +".jar"

    val time = System.currentTimeMillis()

    val manifest = new Manifest()
    manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
    val target = new JarOutputStream(new FileOutputStream(jarPath), manifest)

    db.jarExtra.foreach{ e=>
      val entry = new JarEntry(e.path)
      entry.setTime( time )

      target.putNextEntry( entry )
      target.write( e.data )
      target.closeEntry()

    }

    allClasses.foreach( cl => {
      val path = StringUtils.replace(cl.getName, ".","/") + ".class"
      val stream = cl.getClassLoader.getResourceAsStream(path)
      if (stream == null) {
        throw new RuntimeException("Can not find class:  " + path)
      }

      val entry = new JarEntry(path)
      entry.setTime( time )
      target.putNextEntry( entry )
      val  buffer = new Array[Byte](1024)
      while (stream.available() > 0) {
        val count = stream.read(buffer)
        target.write(buffer, 0, count)
      }
      target.closeEntry()
      stream.close()
    } )

    target.close()

    SchemaInJar(schemaType, schemaVersion, jarPath)
  }
}

case class SchemaInJar(schemaType : String, schemaVersion : String, jarPath : String) {
  def publish( uploadCommand : String ) {

    val cmd = StringUtils.replace( StringUtils.replace(uploadCommand, "${SRC_FILE}", jarPath),
                                   "${SCHEMA_TYPE}", schemaType)


    val cargs = cmd.split("\\s").toSeq
    println(cargs)

    import scala.sys.process._

    val result = cargs ! ProcessLogger( x=> println(x) )
    println("Rsync Exit code: " + result)
    if (result != 0) {
      throw new RuntimeException("Error while uploading")
    }
  }
}
