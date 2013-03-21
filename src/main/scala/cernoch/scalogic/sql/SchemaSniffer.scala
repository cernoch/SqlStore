package cernoch.scalogic.sql

import grizzled.slf4j.Logging
import java.sql.{Date, Types}

import collection.mutable
import math.{BigInt => BigInteger, BigDecimal}
import java.math.{BigInteger => BigInt, BigDecimal => BigDec}
import cernoch.scalogic._

class SchemaSniffer(ada: Adaptor) extends Logging {

	private class Column {
		var canBeNull = false

		object dat {
			var can = false
			var min: Date = null
			var max: Date = null
		}

		object num {
			var can = false
			var min: BigInteger = null
			var max: BigInteger = null
		}

		object dec {
			var can = false
			var min: BigDecimal = null
			var max: BigDecimal = null
		}

		object cat {
			var can = false
			var all = mutable.HashMap[String,Long]()
		}

		def +=(o: Date) = {
			dat.can = true
			if (o != null) {
				if (dat.min == null || dat.min.after(o)) dat.min = o
				if (dat.max == null || dat.max.before(o)) dat.max = o
			} else canBeNull = true
			this
		}

		def +=(o: String) = {
			cat.can = true
			if (o != null)
				cat.all.+=((o, cat.all.get(o).getOrElse(0L) + 1L))
			else
				canBeNull = true
			this
		}

		def +=(o: BigInt) = {
			num.can = true
			if (o != null) {
				if (num.min == null || num.min.bigInteger.compareTo(o) > 0) num.min = o
				if (num.max == null || num.max.bigInteger.compareTo(o) < 0) num.max = o
			} else canBeNull = true
			this
		}

		def +=(o: BigDec) = {
			dec.can = true
			if (o != null) {
				if (dec.min == null || dec.min.bigDecimal.compareTo(o) > 0) dec.min = o
				if (dec.max == null || dec.max.bigDecimal.compareTo(o) < 0) dec.max = o
			} else canBeNull = true
			this
		}

		override def toString =
			s"Dat:${dat.can} => ${dat.min}..${dat.max}; " +
				s"Num:${num.can} => ${num.min}..${num.max}; " +
				s"Dec:${dec.can} => ${dec.min}..${dec.max}; " +
				s"Cat:${cat.can} => ${cat.all}"
	}

	def detectAtoms
	(relation: String, domains: Set[String])
	= {
		import SchemaSniffer._

		debug(s"Detecting schema for relation $relation" +
		      s" and domains ${domains.mkString(", ")}." )

		if (domains.isEmpty) throw new IllegalArgumentException(
			"The list of columns is empty. Nothing to detect." )

		val idx = (domains.view zip Stream.from(1)).toMap
		val cpp = (domains.view map {d => d -> new Column()}).toMap

		val sql =
			s" SELECT ${domains map ada.escapeColumn mkString ", "}" +
			s" FROM ${ada escapeTable relation}"

		ada.withConnection{ ada.query(_, sql, handler = res => {

			val meta = res.getMetaData

			for (name <- domains; i = idx(name); prop = cpp(name)) {
				meta.getColumnType(i) match {
					case Types.DATE  => prop.dat.can = true
					case Types.TIME    => prop.dat.can = true
					case Types.TIMESTAMP => prop.dat.can = true

					case Types.FLOAT   => prop.dec.can = true
					case Types.REAL    => prop.dec.can = true
					case Types.DOUBLE  => prop.dec.can = true
					case Types.NUMERIC => prop.dec.can = true
					case Types.DECIMAL => prop.dec.can = true

					case Types.BIT      => prop.num.can = true
					case Types.TINYINT  => prop.num.can = true
					case Types.SMALLINT => prop.num.can = true
					case Types.INTEGER  => prop.num.can = true
					case Types.BIGINT   => prop.num.can = true

					case Types.CHAR  => prop.cat.can = true
					case Types.NCHAR  => prop.cat.can = true
					case Types.VARCHAR  => prop.cat.can = true
					case Types.NVARCHAR  => prop.cat.can = true
					case Types.LONGVARCHAR => prop.cat.can = true
					case Types.LONGNVARCHAR => prop.cat.can = true
					case v => throw new IllegalArgumentException(
						s"Unsupported column type: ${meta.getColumnTypeName(i+1)}.")
				}
			}

			while (res.next()) {
				for (name <- domains; i = idx(name); prop = cpp(name)) {

					meta.getColumnType(i) match {
						case Types.DATE  => prop += (res getDate i)
						case Types.TIME    => prop += (res getDate i)
						case Types.TIMESTAMP => prop += (res getDate i)

						case Types.FLOAT   => prop += res getBigDecimal i
						case Types.REAL    => prop += res getBigDecimal i
						case Types.DOUBLE  => prop += res getBigDecimal i
						case Types.NUMERIC => prop += res getBigDecimal i
						case Types.DECIMAL => prop += res getBigDecimal i

						case Types.BIT      => prop += round(res getBigDecimal i)
						case Types.TINYINT  => prop += round(res getBigDecimal i)
						case Types.SMALLINT => prop += round(res getBigDecimal i)
						case Types.INTEGER  => prop += round(res getBigDecimal i)
						case Types.BIGINT   => prop += round(res getBigDecimal i)

						case Types.CHAR  => prop += res getString i
						case Types.NCHAR  => prop += res getNString i
						case Types.VARCHAR  => prop += res getString i
						case Types.NVARCHAR  => prop += res getNString i
						case Types.LONGVARCHAR => prop += res getString i
						case Types.LONGNVARCHAR => prop += res getNString i
					}
				}
			}

			for (name <- domains; i = idx(name); prop = cpp(name)) yield {

				if (prop.num.can) {
					debug(s"Column ${name} was recognized as Numeric")
					detectNumeric(name, prop, meta.getColumnType(i))

				} else if (prop.dec.can) {
					debug(s"Column ${name} was recognized as Decimal")
					detectDecimal(name, prop, meta.getColumnType(i))

				} else if (prop.dat.can) {
					debug(s"Column ${name} was recognized as Date")
					detectDate(name, prop, meta.getColumnType(i))

				} else {
					debug(s"Column ${name} was recognized as String")
					detectString(name, prop, meta.getColumnType(i))
				}
			}
		})}
	}

	protected def detectNumeric
	(name: String,
	 prop: Column,
	 tipe: Int)
	: Domain = {

		if (prop.num.min != null &&
		    prop.num.max != null ){

			if (prop.num.min >= Int.MinValue &&
			    prop.num.max <= Int.MaxValue)
				return IntDom(name,
					prop.num.min.toInt,
					prop.num.max.toInt)

			if (prop.num.min >= Long.MinValue &&
			    prop.num.max <= Long.MaxValue)
				return LongDom(name,
					prop.num.min.toLong,
					prop.num.max.toLong)

			return BigIntDom(name,
				prop.num.min,
				prop.num.max)
		}

		LongDom(name)
	}

	protected def detectDecimal
	(name: String,
	 prop: Column,
	 tipe: Int)
	: Domain = {

		if (prop.dec.min != null &&
		    prop.dec.max != null) {

			if (prop.dec.min >= Float.MinValue &&
			    prop.dec.max <= Float.MaxValue)
				return FloatDom(name,
					prop.dec.min.toFloat,
					prop.dec.max.toFloat)

			if (prop.dec.min >= Double.MinValue &&
			    prop.dec.max <= Double.MaxValue )
				return DoubleDom(name,
					prop.dec.min.toDouble,
					prop.dec.max.toDouble)

			return BigDecDom(name,
				prop.dec.min,
				prop.dec.max)
		}

		DoubleDom(name)
	}

	protected def detectDate
	(name: String,
	 prop: Column,
	 tipe: Int)
	: Domain
	= {
		if (prop.dat.min != null &&
			  prop.dat.max != null)
			return DateDom(name,
				prop.dat.min,
				prop.dat.max)

		DateDom(name)
	}

	protected def detectString
	(name: String,
	 prop: Column,
	 tipe: Int)
	: Domain
	= {

		val sum = prop.cat.all.values.sum
		val cnt = prop.cat.all.size

		// All values should be present 10 times in average
		if (sum < (cnt.toLong * 10)) StrDom(name) else
			StrDom.Limited(name, prop.cat.all.keySet.toSet)
	}
}



object SchemaSniffer {
	private[sql] def round(d: BigDec) : BigInt
	= d.setScale(0,BigDec.ROUND_HALF_UP).unscaledValue()
}