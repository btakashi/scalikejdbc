package scalikejdbc

import java.io.InputStream
import java.sql.PreparedStatement
import scalikejdbc.UnixTimeInMillisConverterImplicits._
import scalikejdbc.interpolation.SQLSyntax
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

private[scalikejdbc] case class SQLSyntaxParameterBinder(syntax: SQLSyntax) extends ParameterBinder[SQLSyntax] {
  val value = syntax
  def apply(stmt: PreparedStatement, idx: Int): Unit = ()
}

trait ParameterBinderFactory[A] { self =>

  def apply(value: A): ParameterBinder[A]

  def bimap[B](f: A => B, g: B => A): ParameterBinderFactory[B] = new ParameterBinderFactory[B] {
    def apply(value: B): ParameterBinder[B] = {
      if (value == null) ParameterBinder.NullParameterBinder
      else self(g(value)).map(f)
    }
  }

}

object ParameterBinderFactory extends LowPriorityImplicitsParameterBinderFactory1 {

  def apply[A](f: A => (PreparedStatement, Int) => Unit): ParameterBinderFactory[A] = new ParameterBinderFactory[A] {
    def apply(value: A): ParameterBinder[A] = {
      if (value == null) ParameterBinder.NullParameterBinder
      else ParameterBinder(value, f(value))
    }
  }

  implicit val intParameterBinderFactory: ParameterBinderFactory[Int] = ParameterBinderFactory { v => (ps, idx) => ps.setInt(idx, v) }
  implicit val stringParameterBinderFactory: ParameterBinderFactory[String] = ParameterBinderFactory { v => (ps, idx) => ps.setString(idx, v) }
  implicit val sqlArrayParameterBinderFactory: ParameterBinderFactory[java.sql.Array] = ParameterBinderFactory { v => (ps, idx) => ps.setArray(idx, v) }
  implicit val bigDecimalParameterBinderFactory: ParameterBinderFactory[BigDecimal] = ParameterBinderFactory { v => (ps, idx) => ps.setBigDecimal(idx, v.bigDecimal) }
  implicit val booleanParameterBinderFactory: ParameterBinderFactory[Boolean] = ParameterBinderFactory { v => (ps, idx) => ps.setBoolean(idx, v) }
  implicit val byteParameterBinderFactory: ParameterBinderFactory[Byte] = ParameterBinderFactory { v => (ps, idx) => ps.setByte(idx, v) }
  implicit val sqlDateParameterBinderFactory: ParameterBinderFactory[java.sql.Date] = ParameterBinderFactory { v => (ps, idx) => ps.setDate(idx, v) }
  implicit val doubleParameterBinderFactory: ParameterBinderFactory[Double] = ParameterBinderFactory { v => (ps, idx) => ps.setDouble(idx, v) }
  implicit val floatParameterBinderFactory: ParameterBinderFactory[Float] = ParameterBinderFactory { v => (ps, idx) => ps.setFloat(idx, v) }
  implicit val longParameterBinderFactory: ParameterBinderFactory[Long] = ParameterBinderFactory { v => (ps, idx) => ps.setLong(idx, v) }
  implicit val shortParameterBinderFactory: ParameterBinderFactory[Short] = ParameterBinderFactory { v => (ps, idx) => ps.setShort(idx, v) }
  implicit val sqlXmlParameterBinderFactory: ParameterBinderFactory[java.sql.SQLXML] = ParameterBinderFactory { v => (ps, idx) => ps.setSQLXML(idx, v) }
  implicit val sqlTimeParameterBinderFactory: ParameterBinderFactory[java.sql.Time] = ParameterBinderFactory { v => (ps, idx) => ps.setTime(idx, v) }
  implicit val sqlTimestampParameterBinderFactory: ParameterBinderFactory[java.sql.Timestamp] = ParameterBinderFactory { v => (ps, idx) => ps.setTimestamp(idx, v) }
  implicit val urlParameterBinderFactory: ParameterBinderFactory[java.net.URL] = ParameterBinderFactory { v => (ps, idx) => ps.setURL(idx, v) }
  implicit val utilDateParameterBinderFactory: ParameterBinderFactory[java.util.Date] = sqlTimestampParameterBinderFactory.bimap(identity, _.toSqlTimestamp)
  implicit val jodaDateTimeParameterBinderFactory: ParameterBinderFactory[org.joda.time.DateTime] = utilDateParameterBinderFactory.bimap(_.toJodaDateTime, _.toDate)
  implicit val jodaLocalDateTimeParameterBinderFactory: ParameterBinderFactory[org.joda.time.LocalDateTime] = utilDateParameterBinderFactory.bimap(_.toJodaLocalDateTime, _.toDate)
  implicit val jodaLocalDateParameterBinderFactory: ParameterBinderFactory[org.joda.time.LocalDate] = sqlDateParameterBinderFactory.bimap(_.toJodaLocalDate, _.toDate.toSqlDate)
  implicit val jodaLocalTimeParameterBinderFactory: ParameterBinderFactory[org.joda.time.LocalTime] = sqlTimeParameterBinderFactory.bimap(_.toJodaLocalTime, _.toSqlTime)
  implicit val inputStreamParameterBinderFactory: ParameterBinderFactory[InputStream] = ParameterBinderFactory { v => (ps, idx) => ps.setBinaryStream(idx, v) }
  implicit val nullParameterBinderFactory: ParameterBinderFactory[Null] = new ParameterBinderFactory[Null] { def apply(value: Null) = ParameterBinder.NullParameterBinder }
  implicit val noneParameterBinderFactory: ParameterBinderFactory[None.type] = new ParameterBinderFactory[None.type] { def apply(value: None.type) = ParameterBinder.NullParameterBinder }
  implicit val sqlSyntaxParameterBinderFactory: ParameterBinderFactory[SQLSyntax] = new ParameterBinderFactory[SQLSyntax] { def apply(value: SQLSyntax) = SQLSyntaxParameterBinder(value) }
  implicit val optionalSqlSyntaxParameterBinderFactory: ParameterBinderFactory[Option[SQLSyntax]] = sqlSyntaxParameterBinderFactory.bimap(Option.apply, _ getOrElse SQLSyntax.empty)

}

trait LowPriorityImplicitsParameterBinderFactory1 extends LowPriorityImplicitsParameterBinderFactory0 {

  implicit def optionalParameterBinderFactory[A](implicit ev: ParameterBinderFactory[A]): ParameterBinderFactory[Option[A]] = new ParameterBinderFactory[Option[A]] {
    def apply(value: Option[A]): ParameterBinder[Option[A]] = {
      if (value == null) ParameterBinder.NullParameterBinder
      else value.fold[ParameterBinder[Option[A]]](ParameterBinder.NullParameterBinder)(v => ev(v).map(Option.apply))
    }
  }

  def jsr310ParameterBinderFactory[A]: ParameterBinderFactory[A] = ParameterBinderFactory[A] { p => (underlying, i) =>
    // Accessing JSR-310 APIs via Java reflection
    // because scalikejdbc-core should work on not only Java 8 but 6 & 7.
    import java.lang.reflect.Method
    val className: String = p.getClass.getCanonicalName
    val clazz: Class[_] = Class.forName(className)
    className match {
      case "java.time.ZonedDateTime" | "java.time.OffsetDateTime" =>
        val instant = clazz.getMethod("toInstant").invoke(p) // java.time.Instant
        val dateClazz: Class[_] = Class.forName("java.util.Date") // java.util.Date
        val fromMethod: Method = dateClazz.getMethod("from", Class.forName("java.time.Instant"))
        val dateValue = fromMethod.invoke(null, instant).asInstanceOf[java.util.Date]
        underlying.setTimestamp(i, dateValue.toSqlTimestamp)
      case "java.time.LocalDateTime" =>
        underlying.setTimestamp(i, org.joda.time.LocalDateTime.parse(p.toString).toDate.toSqlTimestamp)
      case "java.time.LocalDate" =>
        underlying.setDate(i, org.joda.time.LocalDate.parse(p.toString).toDate.toSqlDate)
      case "java.time.LocalTime" =>
        underlying.setTime(i, org.joda.time.LocalTime.parse(p.toString).toSqlTime)
    }
  }

}

trait LowPriorityImplicitsParameterBinderFactory0 {
  def anyParameterBinderFactory[A]: ParameterBinderFactory[A] = macro ParameterBinderFactoryMacro.any[A]
}

private[scalikejdbc] object ParameterBinderFactoryMacro {

  def any[A: c.WeakTypeTag](c: Context): c.Expr[ParameterBinderFactory[A]] = {
    import c.universe._
    val A = weakTypeTag[A].tpe
    if (A.toString.startsWith("java.time.")) c.Expr[ParameterBinderFactory[A]](q"scalikejdbc.ParameterBinderFactory.jsr310ParameterBinderFactory[$A]")
    else c.abort(c.enclosingPosition, s"Could not find an implicit value of the ParameterBinderFactory[$A].")
  }

}
