package strd.dynaschema

import org.objectweb.asm._
import scala.collection.mutable
import scala.util.Try

/**
 *
 * User: light
 * Date: 15/04/14
 * Time: 19:05
 */

object ClassWorksHelper {

  import scala.collection.JavaConverters._

  val excludedPackages = Set(
    "scala",
    "java",
    "com.google.protobuf"
  )

  def getCurrentClassLoader: ClassLoader = {
    Thread.currentThread().getContextClassLoader
  }

  def resolveClassName(className: String): Option[Class[_]] = Try { getCurrentClassLoader.loadClass(className) }.toOption

  def fetchDescriptorsFromProtoFile(protoClass: Class[_]) = {
    val fileDescriptor = protoClass.getMethod("getDescriptor").invoke(null).asInstanceOf[com.google.protobuf.Descriptors.FileDescriptor]
    fileDescriptor.getMessageTypes.asScala
  }

  val classesField = classOf[ClassLoader].getDeclaredField("classes") // Vector
  val packagesField = classOf[ClassLoader].getDeclaredField("packages") // HM[String, Package]
  classesField.setAccessible(true)
  packagesField.setAccessible(true)

  def getDependencies(cl: Class[_]): Set[Class[_]] = {
    (resolveScalaClasses(cl) ++
      (getClassParents(cl) ++ asmFetchInners(cl)).flatMap { dep =>
        resolveScalaClasses(dep) + dep
      }
    ).filterNot { dep =>
      excludedPackages.exists(p => dep.getName.startsWith(s"$p."))
    }
  }

  def getAllDependencies(cl: Class[_]): Set[Class[_]] = {
    val deps = new mutable.HashSet[Class[_]]()

    def addDep(dep: Class[_]) {
      if (deps.add(dep)) {
        getDependencies(dep).foreach(addDep)
      }
    }

    addDep(cl)

    deps.toSet
  }

  /**
   * Fetch all superclasses and interfaces recursively
   */
  def getAllClassParents(cl: Class[_]): Set[Class[_]] = getClassParents(cl).flatMap { cl =>
    getAllClassParents(cl) + cl
  }

  /**
   * Get superclass and interfaces
   */
  def getClassParents(cl: Class[_]): Set[Class[_]] =
    (cl.getInterfaces.toSeq ++ Option(cl.getSuperclass).toSeq.filterNot(_ == classOf[Object])).toSet

//  val allPackages = packagesField.get( cl ).asInstanceOf[java.util.HashMap[String,Class[_]]].asScala.toSeq

  /**
   * Fetch compiled scala classes: trait classes, companion classes, ...
   */
  def resolveScalaClasses(cl: Class[_]): Set[Class[_]] = {
    // traits are compiled in interface and class ends with $class
    val tr = if (cl.isInterface) {
      resolveClassName(cl.getName + "$class")
    } else {
      None
    }

    // companion object are compiled as classes end with $
    val comp = resolveClassName(cl.getName + "$")

    comp.toSet ++ tr.toSet
  }

  /**
   * Load all classes loaded with the current one
   */
  def fetchRuntimeDependencies( cl : Class[_], packages: Seq[String] = Nil ) = {

    var classLoader = getCurrentClassLoader

    classLoader.loadClass( cl.getName )

    val allClasses = new mutable.ArrayBuffer[Class[_]]()

    while ( classLoader != null ) {
      val clIter = classesField.get(classLoader).asInstanceOf[java.util.Vector[Class[_]]]

      val firstTry = new java.util.ArrayList[Class[_]]()
      firstTry.addAll(clIter)

      firstTry.asScala.foreach(_.getName)

      val _allClasses = new java.util.ArrayList[Class[_]]()
      _allClasses.addAll(clIter)

      val iter = _allClasses.iterator()
      while (iter.hasNext) {
        allClasses += iter.next()
      }
      classLoader = classLoader.getParent
    }

    allClasses.filter(x => (packages :+ cl.getName).exists(x.getName.startsWith))
  }

  def asmClassName(internal: String) = internal.replace('/', '.')

  /**
   * Find inner/outer classes recursively with ASM
   */
  def asmFetchAllInnerClasses(cl: Class[_]): Seq[Class[_]] = {

    val visitor = new ClassVisitor(Opcodes.ASM4) {
      var classes = mutable.HashSet.empty[String]

      def addClass(name: String) = {
        val className = asmClassName(name)
        if (classes.add(className)) {
          visitClass(className)
        }
      }

      def visitClass(name: String) {
        try {
          new ClassReader(name).accept(this, 0)
        } catch {
          case e: Exception =>
//            println(name)
        }
      }

      override def visitOuterClass(owner: String, name: String, desc: String) = {
        addClass(owner)
      }

      override def visitInnerClass(name: String, outerName: String, innerName: String, access: Int) = {
        addClass(name)
      }
    }

    visitor.visitClass(cl.getName)

    visitor.classes.flatMap(resolveClassName).toSeq
  }

  def asmFetchInners(cl: Class[_]): Set[Class[_]] = {
    val visitor = new ClassVisitor(Opcodes.ASM4) {
      var classes = mutable.ArrayBuffer[String]()

      def addClass(name: String) = {

      }

      def visitClass(name: String) {
        try {
          new ClassReader(name).accept(this, 0)
        } catch {
          case e: Exception =>
          //            println(name)
        }
      }

      override def visitInnerClass(name: String, outerName: String, innerName: String, access: Int) = {
        classes.append(asmClassName(name))
      }

//      override def visitOuterClass(owner: String, name: String, desc: String) {
//        classes.append(asmClassName(name))
//      }
    }

    visitor.visitClass(cl.getName)

    visitor.classes.flatMap(resolveClassName).toSet
  }

  /**
   * Get all dependent classes from parents, methods, fields, etc with ASM
   */
  def getAsmClassDependencies(cl: Class[_], packages: Seq[String] = Nil): Seq[Class[_]] = {
    // TODO: fetch generic classes from signatures

    val visitor = new ClassVisitor(Opcodes.ASM4) {
      val dependencies = mutable.ArrayBuffer[String]()



      def addDependency(name: String) {
        val cl = Option(name).map(asmClassName)
          .filterNot(_.startsWith("scala."))
          .filterNot(_.startsWith("java."))
          .filterNot(_ == classOf[Object].getName)
          .filterNot(dependencies.contains)

        cl.foreach { s =>
          dependencies.append(s)
          visitClass(s)
        }
      }

      def visitClass(cl: String) = {
        try {
          new ClassReader(cl).accept(this, 0)
        } catch {
          case e: Exception =>
            // just ignore wrong classes
        }
      }

      override def visit(version: Int, access: Int, name: String, signature: String, superName: String, interfaces: Array[String]) {
        addDependency(superName)
        interfaces.foreach(addDependency)
      }

      override def visitOuterClass(owner: String, name: String, desc: String) {
        addDependency(owner)
        addDesc(desc)
      }

      override def visitInnerClass(name: String, outerName: String, innerName: String, access: Int) {
        addDependency(name)
      }

      override def visitField(access: Int, name: String, desc: String, signature: String, value: scala.Any): FieldVisitor = {
        if (signature == null) {
          addDesc(desc)
        } else {
//          addTypeSignature(signature)
        }

        value match {
          case t: Type =>
            addType(t)
          case _ =>
        }

        null
      }


      override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]): MethodVisitor = {
        if (signature == null) {
          addMethodDesc(desc)
        } else {
//          addSignature(signature)
        }

        Option(exceptions).foreach(_.foreach(addDependency))

        null
      }

      def addDesc(desc: String) {
        addType(Type.getType(desc))
      }

      def addType(t: Type) {
        t.getSort match {
          case Type.ARRAY =>
            addType(t.getElementType)
          case Type.OBJECT =>
            addDependency(t.getInternalName)
          case Type.METHOD =>
            addType(t.getReturnType)
            t.getArgumentTypes.map(addType)
          case _ =>
        }
      }

//      def addTypeSignature(signature: String) {
//        if (signature != null) {
//          new SignatureReader(signature).acceptType(this)
//        }
//      }

      def addMethodDesc(desc: String) {
        addType(Type.getReturnType(desc))
        Type.getArgumentTypes(desc).foreach(addType)
      }

//      def addSignature(signature: String) {
//        if (signature != null) {
//          new SignatureReader(signature).accept(this)
//        }
//      }

    }

    visitor.visitClass(cl.getName)

    visitor.dependencies.filter(x => (packages :+ cl.getName).exists(x.startsWith)).flatMap(resolveClassName)
  }
}
