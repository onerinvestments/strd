package strd.pg

import org.squeryl.adapters.PostgreSqlAdapter
import org.squeryl.internals.{StatementWriter, FieldMetaData}
import org.squeryl.{ReferentialAction, Table, Schema}

/**
 *
 */
class AwesomePostgreSqlAdapter extends PostgreSqlAdapter {

  override def usePostgresSequenceNamingScheme: Boolean = true

  override def writeColumnDeclaration(fmd: FieldMetaData, isPrimaryKey: Boolean, schema: Schema): String = {
    val dbTypeDeclaration = databaseTypeFor(fmd)

    val sb = new StringBuilder(128)

    sb.append("  ")
    sb.append(quoteName(fmd.columnName))
    sb.append(" ")
    sb.append(dbTypeDeclaration)

    for(d <- fmd.defaultValue) {
      sb.append(" default ")

      val v = convertToJdbcValue(d.value.asInstanceOf[AnyRef])
      if(v.isInstanceOf[String])
        sb.append("'" + v + "'")
      else
        sb.append(v)
    }

    if(isPrimaryKey)
      sb.append(" primary key")

    if(!fmd.isOption)
      sb.append(" not null")

    sb.toString()
  }


  override def postCreateTable(t: Table[_], printSinkWhenWriteOnlyMode: Option[(String) => Unit]): Unit = {}


  override def postDropTable(t: Table[_]): Unit = {}

  override def databaseTypeFor(fmd: FieldMetaData): String = {
    fmd.explicitDbTypeDeclaration.getOrElse(
      if (fmd.isAutoIncremented) "serial" else super.databaseTypeFor(fmd)
    )
  }


  override def writeForeignKeyDeclaration(foreignKeyTable: Table[_], foreignKeyColumnName: String,
                                          primaryKeyTable: Table[_], primaryKeyColumnName: String,
                                          referentialAction1: Option[ReferentialAction],
                                          referentialAction2: Option[ReferentialAction], fkId: Int): String = {

    val foreignKeyCols =
      foreignKeyTable.posoMetaData.fieldsMetaData.filter(fmd => fmd.columnName == foreignKeyColumnName).toSeq

    val dec = super.writeForeignKeyDeclaration(
      foreignKeyTable, foreignKeyColumnName,
      primaryKeyTable, primaryKeyColumnName,
      referentialAction1, referentialAction2, fkId
    )

    if (foreignKeyCols.head.declaredAsPrimaryKeyInSchema) {
      dec
    } else {
      dec + ";" + writeIndexDeclaration(foreignKeyCols, Some(s"${foreignKeyTable.name}_${foreignKeyColumnName}_idx"), None, isUnique = false)
    }
  }
}
