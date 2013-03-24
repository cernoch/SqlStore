package cernoch.scalogic.sql

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import cernoch.scalogic._

@RunWith(classOf[JUnitRunner])
class SchemaSnifferTest extends Specification {

	def fib( n: Int): Int = n match {
		case 0 | 1 => n
		case   _   => fib( n -1) + fib( n-2)
	}

	def fillData(ada: Adaptor) = {
		ada.withConnection(con => {

			con.prepareStatement(
				"CREATE TABLE \"tabulka\" (\n"
					+ " \"smallint\" INT,\n"
					+  " \"integer\" INTEGER,\n"
					+     " \"char\" CHAR,\n"
					+   " \"string\" VARCHAR(200),\n"
					+   " \"double\" DOUBLE,\n"
					+   "   \"date\" TIMESTAMP,\n"
					+   " \"bigdec\" NUMERIC(30)\n)"
			).execute()

			val smallint = Stream.from(1,10).map{_.toShort}.iterator
			val integer = Stream.from(1000,100).iterator
			val char   = Stream.from(64,1).map{_.toChar}.iterator
			val double = Stream.from(0,1).map(fib).map{_.toDouble}.iterator
			val string = Stream.from(0,1).map(fib).map{_.toString}.iterator
			val date   = Stream.from(16000,1).map{_.toLong * 24 * 60 * 60 * 1000}.iterator
			val bigDec = Stream.from(1,1).map{BigDecimal(2).pow(_)}.iterator

			for (row <- 1 to 32) {
				val st = con.prepareStatement("INSERT INTO \"tabulka\" VALUES ( ?,?,?,?,?,?,? )")
				st.setShort (     1, smallint.next())
				st.setInt   (     2, integer.next())
				st.setString(     3, char.next().toString)
				st.setString(     4, string.next())
				st.setDouble(     5, double.next())
				st.setDate  (     6, new java.sql.Date(date.next()))
				st.setBigDecimal( 7, bigDec.next().bigDecimal)
				st.execute()
			}

		})
		ada
	}


	"Schema sniffer" should {
		val ada = fillData(new DerbyMemAdaptor("sniffer"))
		val snf = new SchemaSniffer(ada)
		val col = Set("smallint", "integer", "char", "string", "double", "date", "bigdec")
		val det = snf.detectAtoms("tabulka", col)
		val map = det.map{d => d.name -> d}.toMap

		def throwException(d: Domain) = throw new Exception(
			s"Domain '$d' has an incorrect type.")

		map("smallint") match {
			case IntDom("smallint",_) =>
			case _ => throwException(_)
		}

		map("integer") match {
			case IntDom("integer",_) =>
			case _ => throwException(_)
		}
		map("char") match {
			case StrDom("char") =>
			case _ => throwException(_)
		}

		map("string") match {
			case StrDom("string") =>
			case _ => throwException(_)
		}

		map("double") match {
			case FloatDom("double",_) =>
			case _ => throwException(_)
		}

		map("date") match {
			case DateDom("date",_) =>
			case _ => throwException(_)
		}

		map("bigdec") match {
			case BigDecDom("bigdec",_) =>
			case _ => throwException(_)
		}

		true
	}
}
