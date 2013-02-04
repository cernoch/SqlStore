package cernoch.sm.sql

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import cernoch.scalogic._
import numeric.LessThan

@RunWith(classOf[JUnitRunner])
class SqlTest extends Specification {

  object Dom {
    val person = CatDom("person", true)

    val car = NumDom("car", true)
    val make = CatDom("make", false, Set("skoda", "smart"))
    val color = CatDom("color", false, Set("red", "white"))
    val doors = NumDom("doors")

    val all = List(person, car, make, color, doors).toSet
  }

  object Obj {

    import Dom._

    val franta = new Cat("franta", person)
    val pepa   = new Cat("pepa", person)

    val red   = new Cat("red", color)
    val white = new Cat("white", color)

    val skoda = new Cat("skoda", make)
    val smart = new Cat("smart", make)

    val redForTwo   = new Num(1, car)
    val whiteForTwo = new Num(2, car)
    val skodaFabia  = new Num(3, car)

    val twoDoors  = new Num(2, doors)
    val fourDoors = new Num(4, doors)
  }

  object Var {
    val car1 = new Var(Dom.car);
    val car2 = new Var(Dom.car);
    val car3 = new Var(Dom.car);
    val make1 = new Var(Dom.make);
    val make2 = new Var(Dom.make);
    val make3 = new Var(Dom.make);
    val doors1 = new Var(Dom.doors);
    val doors2 = new Var(Dom.doors);
    val doors3 = new Var(Dom.doors);
    val color1 = new Var(Dom.color);
    val color2 = new Var(Dom.color);
    val color3 = new Var(Dom.color);
    val person1 = new Var(Dom.person);
    val person2 = new Var(Dom.person);
    val person3 = new Var(Dom.person);
  }

  object Sch {

    import Var._

    val cars = new Btom("car", List(car1, make1, color1, doors1), Set(), Set())
    val people = new Btom("man", List(person1), Set(), Set())
    val ownership = new Btom("owns", List(person1, car1), Set(), Set())

    val all = List(BLC(cars), BLC(people), BLC(ownership))
  }

  def fixtures(s: SqlStorage) = {
    import Var._
    import Obj._
    import Sch._

    val importer = s.reset

    def input
      (x: Atom[Val[_]])
    = importer.put(BLC(x))

    // Cars

    input(cars.mapAllArgs(Map(
      car1   -> redForTwo,
      make1  -> smart,
      color1 -> red,
      doors1 -> twoDoors
    )))

    input(cars.mapAllArgs(Map(
      car1   -> whiteForTwo,
      make1  -> smart,
      color1 -> white,
      doors1 -> twoDoors
    )))

    input(cars.mapAllArgs(Map(
      car1   -> skodaFabia,
      make1  -> skoda,
      color1 -> white,
      doors1 -> fourDoors
    )))

    // People

    input(people.mapAllArgs(Map(
      person1 -> franta
    )))

    input(people.mapAllArgs(Map(
      person1 -> pepa
    )))

    // Ownership

    input(ownership.mapAllArgs(Map(
      person1 -> pepa,
      car1    -> redForTwo
    )))

    input(ownership.mapAllArgs(Map(
      person1 -> pepa,
      car1    -> whiteForTwo
    )))

    input(ownership.mapAllArgs(Map(
      person1 -> franta,
      car1    -> whiteForTwo
    )))

    input(ownership.mapAllArgs(Map(
      person1 -> franta,
      car1    -> skodaFabia
    )))

    importer.close
  }



  "SQL storage" should {

    "import data without an exception" in {
      val storage
      = new SqlStorage(
        new DerbyInMemory("test1"),
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
          new DerbyInMemory("test2"),
          Sch.all) )

      val q = new Horn(
        Atom("head", List[FFT](person1, doors1)),
        Set(
          (people.asInstanceOf[Atom[Var]]),
          (ownership),
          (cars mapSomeArg Dict(color1 -> white).get)
        )
      )

      engine.query(q).toSet.map{
        (m: Var => Val[_]) => (m(person1),m(doors1))
      } must_== Set(
        (pepa,   twoDoors),
        (franta, twoDoors),
        (franta, fourDoors)
      )
    }

    "query same table twice" in {
      import Var._
      import Obj._
      import Sch._

      val engine
      = fixtures(
        new SqlStorage(
          new DerbyInMemory("test3"),
          Sch.all) )

      val q = new Horn(
        Atom("head", List[FFT](person1, person2)),
        Set(
          (ownership.asInstanceOf[Atom[Var]]),
          (ownership mapSomeArg Dict[FFT](person1 -> person2).get)
        )
      )

      engine.query(q).toSet
        .map{(m:Var=>Val[_]) => (m(person1),m(person2)) }
        .filter{ case(x,y) => x != y } must_==
        Set(
          (pepa, franta),
          (franta, pepa)
        )
    }



    "handle variable inequalities" in {
      import Var._
      import Obj._
      import Sch._

      val engine
      = fixtures(
        new SqlStorage(
          new DerbyInMemory("test4"),
          Sch.all) )

      val q = new Horn(
        Atom("head", List[FFT](car1)),
        Set(
          cars,
          new LessThan(doors1, fourDoors)
        )
      )

      engine.query(q).toSet
        .map{(m:Var=>Val[_]) => m(car1)} must_==
          Set(whiteForTwo, redForTwo)
    }
  }
}