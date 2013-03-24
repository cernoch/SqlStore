package cernoch.scalogic.sql

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import cernoch.scalogic._

@RunWith(classOf[JUnitRunner])
class BugBasedTests extends Specification {

	object Dom {
		val make  = StrDom.Limited("make", Set("skoda", "smart"))
		val color = StrDom.Limited("color", Set("red", "white"))
		val door  = IntDom("doors")
		val age   = DoubleDom("age")

		val all = List(make, color, door, age)
	}

	object Obj {
		import Dom._

		val skoda = Val("skoda", make)
		val smart = Val("smart", make)
		val makes = List(skoda, smart)

		val red   = Val("red", color)
		val white = Val("white", color)
		val colors = List(red, white)

		val twoDoors  = Val(2, door)
		val fourDoors = Val(4, door)
		val doors = List(twoDoors, fourDoors)

		val age1 = Val(1.0, age)
		val age2 = Val(1.2, age)
		val age3 = Val(2.0, age)
		val age4 = Val(2.4, age)
		val age5 = Val(3.0, age)
		val ages = List(age1, age2, age3, age4, age5)

		val all = List(makes, colors, doors, ages)
	}

	object Vars {
		val make = new Var(Dom.make)
		val color = new Var(Dom.color)
		val doors = new Var(Dom.door)
		val age = new Var(Dom.age)

		val all = List(make, color, doors, age)
	}

	val btom = Atom("car", Vars.all)

	/** Carthesian product of lists */
	def carthesian
	[T]
	(data: List[List[T]])
	: List[List[T]]
	= data match {
		case Nil => List(Nil)
		case head :: tail =>
			for(xh <- head;
					xt <- carthesian(tail))
			yield xh :: xt
	}

	def column[T]
	(l: List[List[T]])
	: (List[T], List[List[T]])
	= l match {
		case Nil => (Nil, Nil)
		case Nil :: rows => (Nil, Nil)
		case (item :: rowRest) :: rows
		=> column(rows) match {
			case (col,strippedRows)
			=> (item :: col, rowRest :: strippedRows)
		}
	}

	def transpose[T]
	(l: List[List[T]])
	: List[List[T]]
	= column(l) match {
		case (Nil, Nil) => List()
		case (col, rows) => col :: transpose(rows)
	}


	private def fixtures(s: SqlStorage) = {
		val im = s.reset()

		for (args <- carthesian(Obj.all))
			im.put(Atom("car", args))

		im.done()
	}

	def mkVars(d: Domain)(n: Int) : List[List[Var]]
	= n match {
		case 0 => List(List(Var(d)))
		case _ => mkVars(d)(n-1).flatMap{prev =>
			List(prev.head :: prev, Var(d) :: prev)
		}
	}



	"Internal validation" should {
		"extract column" in {
			column(List(List(1,2),List(3,4))) must_== (List(1,3), List(List(2),List(4)))
		}

		"transpose a matrix" in {
			transpose(List(List(1,2),List(3,4))) must_== List(List(1,3),List(2,4))
		}

		"transpose a 0x matrix" in { transpose(List()) must_== List() }
		"transpose a 2x0 matrix" in { transpose(List(Nil,Nil)) must_== List() }
		"transpose a Null matrix" in { transpose(Nil) must_== Nil }
	}

	"SQL storage on large data" should {

		"import data without an exception" in {
			fixtures(new SqlStorage( new DerbyMemAdaptor("bug1"), List(btom)))
			true
		}

		"execute arbitrary query of two atoms" in {
			val engine = fixtures(new SqlStorage(new DerbyMemAdaptor("bug2"), List(btom)))

			for (conjuncts <- 0 to 1) {
				for (inst <- carthesian(
					Dom.all.map(mkVars)
						.map{f => transpose(f(conjuncts))})
				) {
					val body = transpose(inst).map(Atom("car", _))
					val vars = body.flatMap{_.vars}.distinct
					engine.query(
						new Horn( Atom("head", vars), body.toSet[Atom] ),
						mapa => vars.map(mapa(_)) )
				}
			}
			true
		}
	}
}