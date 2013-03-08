package cernoch.sm.sql

import jdbc.DerbyMemAdaptor
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import cernoch.scalogic._

@RunWith(classOf[JUnitRunner])
class SqlTest extends Specification {

  object Dom {
		val person = Domain.cat("person")
		val car    = Domain.int("car")
		val make   = Domain.cat("make", Set("skoda", "smart"))
		val color  = Domain.cat("color",Set("red", "white"))
		val doors  = Domain.int("doors")
		val age    = Domain.dec("age")


    val all = List(person, car, make, color, doors).toSet
  }

  object Obj {

    import Dom._

    val franta = Val("franta", person)
    val pepa   = Val("pepa", person)

    val red   = Val("red", color)
    val white = Val("white", color)

    val skoda = Val("skoda", make)
    val smart = Val("smart", make)

    val redForTwo   = Val(1, car)
    val whiteForTwo = Val(2, car)
    val skodaFabia  = Val(3, car)

    val twoDoors  = Val(2, doors)
    val fourDoors = Val(4, doors)
  }

  object Var {
    val car1 = new Var(Dom.car)
    val car2 = new Var(Dom.car)
    val car3 = new Var(Dom.car)
    val make1 = new Var(Dom.make)
    val make2 = new Var(Dom.make)
    val make3 = new Var(Dom.make)
    val doors1 = new Var(Dom.doors)
    val doors2 = new Var(Dom.doors)
    val doors3 = new Var(Dom.doors)
    val color1 = new Var(Dom.color)
    val color2 = new Var(Dom.color)
    val color3 = new Var(Dom.color)
    val person1 = new Var(Dom.person)
    val person2 = new Var(Dom.person)
    val person3 = new Var(Dom.person)
  }

  object Sch {

    import Var._

    val cars = Atom("car", car1, make1, color1, doors1)
    val people = Atom("man", person1)
    val ownership = Atom("owns", person1, car1)

    val all = List(cars, people, ownership)
  }

  private def fixtures(s: SqlStorage) = {
    import Var._
    import Obj._
    import Sch._

    val importer = s.reset()

    def input(x: Atom) {
      importer.put(x)
    }

    // Cars

		importer.put(cars.subst(
      car1   -> redForTwo,
      make1  -> smart,
      color1 -> red,
      doors1 -> twoDoors
    ))

		importer.put(cars.subst(
      car1   -> whiteForTwo,
      make1  -> smart,
      color1 -> white,
      doors1 -> twoDoors
    ))

		importer.put(cars.subst(
      car1   -> skodaFabia,
      make1  -> skoda,
      color1 -> white,
      doors1 -> fourDoors
    ))

    // People

		importer.put(people.subst(
      person1 -> franta
    ))

		importer.put(people.subst(
      person1 -> pepa
    ))

    // Ownership

		importer.put(ownership.subst(
      person1 -> pepa,
      car1    -> redForTwo
    ))

		importer.put(ownership.subst(
      person1 -> pepa,
      car1    -> whiteForTwo
    ))

		importer.put(ownership.subst(
      person1 -> franta,
      car1    -> whiteForTwo
    ))

		importer.put(ownership.subst(
      person1 -> franta,
      car1    -> skodaFabia
    ))

    importer.done()
  }



  "SQL storage" should {

    "import data without an exception" in {
      val storage
      = new SqlStorage(
        new DerbyMemAdaptor("test1"),
        Sch.all)

      fixtures(storage)

      true
    }

    "execute simple query correctly" in {
      import Var._
      import Obj._
      import Sch._

      val engine
      = fixtures(
        new SqlStorage(
          new DerbyMemAdaptor("test2"),
          Sch.all) )

      val q = Horn(
        Atom("head", person1, doors1),
        Set(
          (people.atom),
          (ownership.atom),
          (cars.atom.subst(color1 -> white))
        )
      )

      val out = collection.mutable.HashSet[List[Val]]()

      engine.query(q, mapa => { out.add(q.head.vars.map(mapa)) })
      out must_== Set(
        List(pepa,   twoDoors),
        List(franta, twoDoors),
        List(franta, fourDoors)
      )
    }



    "query same table twice" in {
      import Var._
      import Obj._
      import Sch._

      val engine = fixtures(new SqlStorage(new DerbyMemAdaptor("test3"), Sch.all))

      val q = new Horn(
        Atom("head", person1, car1),
        Set[Atom](
          ownership,
          ownership.subst(car1 -> redForTwo)
        )
      )

      val out = collection.mutable.HashSet[List[Val]]()

      engine.query(q, mapa => {
        val list = q.head.vars.map(mapa)
        if (list(0) != list(1)) out.add(list)
      })
      out must_== Set(
        List(pepa, redForTwo),
        List(pepa, whiteForTwo)
      )
    }



    "handle variable inequalities" in {
      import Var._
      import Obj._
      import Sch._

      val engine = fixtures(new SqlStorage(new DerbyMemAdaptor("test4"), Sch.all))

      val q = Horn( Atom("head", car1),
        Set(cars, Atom("<", doors1, fourDoors)) )

      val out = collection.mutable.HashSet[Val]()
      engine.query( q, mapa => out.add(mapa(car1)) )
      out must_== Set(whiteForTwo, redForTwo)
    }
  }
}