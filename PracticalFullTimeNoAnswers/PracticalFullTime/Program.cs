using System;
using System.Collections.Generic;
using System.Linq;

namespace FirstExam
{
  public class Travel
  {
    public string Code { get; set; }
    public string Departure { get; set; }
    public string Destination { get; set; }
    public DateTime Date { get; set; }

    public Travel(string code, string departure, string destination, DateTime date)
    {
      Code = code;
      Departure = departure;
      Destination = destination;
      Date = date;
    }
  }

  public class Customer
  {
    public int Id { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public byte Age { get; set; }

    public Customer(int id, string firstName, string lastName, byte age)
    {
      Id = id;
      FirstName = firstName;
      LastName = lastName;
      Age = age;
    }
  }

  public class CustomerTravel
  {
    public int CustomerId { get; set; }
    public string TravelCode { get; set; }

    public CustomerTravel(int customerId, string travelCode)
    {
      CustomerId = customerId;
      TravelCode = travelCode;
    }
  }

  public static class MapReduce
  {
    public static IEnumerable<T2> Map<T1, T2>(this IEnumerable<T1> collection, Func<T1, T2> transformation)
    {
      T2[] result = new T2[collection.Count()];
      for (int i = 0; i < collection.Count(); i++)
      {
        //TODO 1: complete the implementation of map
      }
      return result;
    }

    public static T2 Reduce<T1, T2>(this IEnumerable<T1> collection, T2 init, Func<T2, T1, T2> operation)
    {
      T2 result = init;
      for (int i = 0; i < collection.Count(); i++)
      {
        result = operation(result, collection.ElementAt(i));
      }
      return result;
    }

    public static IEnumerable<Tuple<T1, T2>> Join<T1, T2>(this IEnumerable<T1> table1, IEnumerable<T2> table2, Func<T1, T2, bool> condition)
    {
      return
        Reduce(table1, new List<Tuple<T1, T2>>(),
          (queryResult, x) =>
          {
            List<Tuple<T1, T2>> combination =
              Reduce(table2, new List<Tuple<T1, T2>>(),
                      (c, y) =>
                      {
                        //TODO 2: complete the implementation of join
                        return c;
                      });
            queryResult.AddRange(combination);
            return queryResult;
          });
    }
  }

  class Program
  {
    public static Travel[] TravelTable =
    {
      new Travel("XADSD", "Venice", "Rottedam", new DateTime(2019, 2, 10)),
      new Travel("AAZSD", "New York", "Paris", new DateTime(2020, 8, 15)),
      new Travel("DDSAS", "Vancouver", "Oslo", new DateTime(2019, 3, 15)),
      new Travel("VVSVX", "Hamburg", "Moscow", new DateTime(2021, 6, 21)),
      new Travel("PPSH4", "Cape Town", "Cairo", new DateTime(2019, 8, 30)),
      new Travel("XXSAS", "London", "Madrid", new DateTime(2018, 12, 20))
    };

    public static Customer[] CustomerTable =
    {
      new Customer(0, "John", "Doe", 35),
      new Customer(1, "Jane", "Doe", 28),
      new Customer(2, "Max", "Payne", 45),
      new Customer(3, "Lara", "Croft", 25),
      new Customer(4, "Rachel", "Jackson", 28),
      new Customer(5, "Michael", "Knight", 40)
    };

    public static CustomerTravel[] CustomerTravelTable =
    {
      new CustomerTravel(0, "XADSD"),
      new CustomerTravel(0, "AAZSD"),
      new CustomerTravel(1, "XADSD"),
      new CustomerTravel(1, "AAZSD"),
      new CustomerTravel(2, "PPSH4"),
      new CustomerTravel(2, "AAZSD"),
      new CustomerTravel(2, "XADSD"),
      new CustomerTravel(2, "XXSAS"),
      new CustomerTravel(3, "DDSAS"),
      new CustomerTravel(4, "DDSAS"),
      new CustomerTravel(4, "AAZSD"),
      new CustomerTravel(4, "XXSAS")
    };

    static void Main(string[] args)
    {
      /*
       * SELECT Departure, Destination
       * FROM Travel
       * WHERE Travel.Date < '2019-06-21'
       */

      var q1 = 
        TravelTable.Reduce(
        new List<Travel>(),
        (filteredTravels, travel) =>
        {
          //TODO 3: complete the implementation of query 1
          return filteredTravels;
        }
        ).Map(t => new { t.Departure, t.Destination });

      /*
       * SELECT c.FirstName, c.LastName, t.Destination
       * FROM Customer c INNER JOIN CustomerTravel ct
       * ON c.Id = ct.CustomerId INNER JOIN Travel t
       * ON t.Code = ct.TravelCode
       */

      var q2 =
        CustomerTable.Join(
          CustomerTravelTable,
          (c, ct) => c.Id == ct.CustomerId
          )
        .Join(
          //TODO 4a: complete the implementation of query 2
          )
        .Map(t => /* //TODO 4a: complete the implementation of query 2 */);

      /*
       * SELECT c.FirstName, c.LastName, t.Destination
       * FROM Customer c INNER JOIN CustomerTravel ct
       * ON c.Id = ct.CustomerId INNER JOIN Travel t
       * ON t.Code = ct.TravelCode
       * WHERE t.Date > '2019-02-01'
       */

      var q3 =
        CustomerTable.Join(
          CustomerTravelTable,
          (c, ct) => c.Id == ct.CustomerId
          )
        .Join(
          //TODO 5a: complete the implementation of query 3
          )
        .Map(t => /* TODO 5b: complete the implementation of query 3 */);

    }
  }
}
