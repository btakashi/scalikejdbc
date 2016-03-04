package scalikejdbc

import java.sql.PreparedStatement

/**
 * EssentialParameterBinder which enables customizing StatementExecutor#binParams.
 *
 * {{{
 * val bytes = Array[Byte](1,2,3, ...)
 * val in = ByteArrayInputStream(bytes)
 * val bin = ParameterBinder(
 *   value = in,
 *   binder = (stmt, idx) => stmt.setBinaryStream(idx, in, bytes.length)
 * )
 * sql"insert into table (bin) values (${bin})".update.apply()
 * }}}
 */
trait EssentialParameterBinder { self =>

  type ValueType

  def value: ValueType

  /**
   * Applies parameter to PreparedStatement.
   */
  def apply(stmt: PreparedStatement, idx: Int): Unit

  override def toString: String = s"ParameterBinder(value=$value)"
}
trait ParameterBinder[A] extends EssentialParameterBinder { self =>
  type ValueType = A
  def map[B](f: A => B): ParameterBinder[B] = new ParameterBinder[B] {
    lazy val value: B = f(self.value)
    def apply(stmt: PreparedStatement, idx: Int): Unit = self(stmt, idx)
  }
}

/**
 * ParameterBinder factory.
 */
object ParameterBinder {

  /**
   * Factory method for ParameterBinder.
   */
  def apply[A](value: A, binder: (PreparedStatement, Int) => Unit): ParameterBinder[A] = {
    val _value = value
    new ParameterBinder[A] {
      val value: A = _value
      override def apply(stmt: PreparedStatement, idx: Int): Unit = binder(stmt, idx)
    }
  }

  def unapply(a: Any): Option[Any] = {
    PartialFunction.condOpt(a) {
      case x: EssentialParameterBinder => x.value
    }
  }

  def NullParameterBinder[A]: ParameterBinder[A] = new ParameterBinder[A] {
    val value = null.asInstanceOf[A]
    def apply(stmt: PreparedStatement, idx: Int): Unit = stmt.setObject(idx, null)
    override def toString: String = s"ParameterBinder(value=NULL)"
  }

}

