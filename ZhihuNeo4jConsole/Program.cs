using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Neo4j.Driver.V1;
using MySql.Data.MySqlClient;

namespace ZhihuNeo4jConsole
{
    class Program
    {
        //private static System.IO.FileInfo[] filelist;
        private static System.IO.DirectoryInfo folder;
        private static System.IO.FileInfo[] fileList;
        static void Main(string[] args)
        {
            using (var person = new LoadCSVFile("bolt://localhost:7687", "neo4j", "root"))
            {
                folder = new System.IO.DirectoryInfo(@"C:\Users\JC\AppData\Roaming\Neo4j Desktop\Application\neo4jDatabases\database-0eceb62c-6897-4d6d-a0a8-32d691ee48bd\installation-3.3.2\import");
                fileList = folder.GetFiles();
                foreach (var file in fileList)
                    person.PrintPeople(file.Name);
                Console.WriteLine("Success");
                Console.ReadLine();
            }

        }
    }

    public class LoadCSVFile : IDisposable
    {
        private readonly IDriver _driver;
        private MySqlConnection connection = new MySqlConnection("Host=localhost;user=root;password=root;database=userinfo;charset=utf8");
        private MySqlCommand command;
        private MySqlDataReader reader;
        //private string sql;
        private string id;
        //private string name;
        

        public LoadCSVFile(string uri, string user, string password)
        {
            _driver = GraphDatabase.Driver(uri, AuthTokens.Basic(user, password));
            
        }

        public void PrintPeople(string file)
        {
            string url_token = file.Replace("_following.csv", "");

            var session = _driver.Session();

            /*Only Run Once...
            session.WriteTransaction(tx =>
            {
                string top2000File = "Top2000.csv";
                string query = "load csv with headers from \"file:///" + top2000File + "\" AS line merge(p:person{ name: line.name,url_token: line.url_token,id: line.id})";
                tx.Run(query);
            });
            return;
            */
            session.WriteTransaction(tx =>
            {
                tx.Run("create constraint on (n:person) assert n.id is unique;");
            });

            url_token = file.Replace("_following.csv", "");
            string sql = "select id from top2000 where url_token = \"" + url_token + "\"";
            if (connection.State == System.Data.ConnectionState.Closed) connection.Open();
            command = new MySqlCommand(sql, connection);
            reader = command.ExecuteReader();
            while (reader.Read())
            {
                id = reader[0].ToString();

            }
            reader.Close();

            try
            {
                session.WriteTransaction(tx =>
                {
                    string query = "load csv with headers from \"file:///" + file + "\" AS line " +
                    "match(n:person) where n.id = \"" + id + "\" " +
                    "match(p:person{ name: line.name,url_token: line.url_token,id: line.id})" +
                    "create (n)-[:FOLLOWS]->(p)";
                    tx.Run(query);
                });
            }
            catch (Exception e)
            {
                Console.Write(e.Message);
            }
        }

        public void Dispose()
        {
            _driver?.Dispose();
        }
    }

    public class HelloWorldExample : IDisposable
    {
        private readonly IDriver _driver;

        public HelloWorldExample(string uri, string user, string password)
        {
            _driver = GraphDatabase.Driver(uri, AuthTokens.Basic(user, password));
        }

        public void PrintPeople(string title)
        {
            using (var session = _driver.Session())
            {
                var person = session.WriteTransaction(tx =>
                {
                    var result = tx.Run("CREATE (a:Person) " +
                                        "SET a.title = title " +
                                        "RETURN a.title + ', from node ' + id(a)",
                        new { title });
                    return result.Single()[0].As<string>();
                });
                Console.WriteLine(person);
            }
        }

        public void Dispose()
        {
            _driver?.Dispose();
        }
    }
}
