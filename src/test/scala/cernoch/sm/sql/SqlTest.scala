package cernoch.sm.sql

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import cernoch.scalogic._
import java.sql.DriverManager

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

  val derbySettings = new SqlSettings() {
    driver = "derby:memory"
    clName = "org.apache.derby.jdbc.EmbeddedDriver"

    override def connUrl = super.connUrl + ";create=true"

    override def apply()
    = {
      Class.forName(clName).newInstance()
      DriverManager.getConnection(connUrl)
    }

    override def domainName
    (d: Domain[_])
    = d match {
      case DecDom(_) => "DOUBLE PRECISION"
      case NumDom(_,_) => "BIGINT"
      case CatDom(_,_,_) => "VARCHAR(250)"
    }

  }

  "SQL storage" should {
    "import and query data" in {
      import Var._
      import Obj._
      import Sch._

      val store = new SqlStorage(derbySettings, Sch.all)

      val importer = store.reset

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

      val engine = importer.close



      val res1 = engine.query(new Horn(
        Atom("head", List[FFT](person1, doors1)),
        Set(
          (people.asInstanceOf[Atom[Var]]),
          (ownership),
          (cars mapSomeArg Dict(color1 -> white).get)
        )
      ))
      .toSet
      .map{
        (m: Var => Val[_]) => (m(person1),m(doors1))
      } must_== Set(
        (pepa,   twoDoors),
        (franta, twoDoors),
        (franta, fourDoors)
      )

      val res2 = engine.query(new Horn(
        Atom("head", List[FFT](person1, person2)),
        Set(
          (ownership.asInstanceOf[Atom[Var]]),
          (ownership mapSomeArg Dict[FFT](person1 -> person2).get)
        )
      ))
      .toSet
      .map{(m:Var=>Val[_]) => (m(person1),m(person2)) }
      .filter{ case(x,y) => x != y } must_==
      Set(
        (pepa, franta),
        (franta, pepa)
      )

      res1 and res2
    }
  }
}