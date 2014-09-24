using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;

namespace ggkimport
{
    class Program
    {
        static string SQLITE_DB = "ggk.db";

        public const int AUTOSOMAL_FTDNA = 0;
        public const int AUTOSOMAL_23ANDME = 1;
        public const int AUTOSOMAL_ANCESTRY = 2;
        public const int AUTOSOMAL_DECODEME = 3;
        public const int AUTOSOMAL_GENO2 = 4;

        static void Main(string[] args)
        {

            if(args.Length!=4)
            {
                Console.WriteLine("Genetic Genealogy Kit (GGK) Console Importer");
                Console.WriteLine("--------------------------------------------");
                Console.WriteLine("Website: y-str.org");
                Console.WriteLine("Developer: Felix Chandrakumar");
                Console.WriteLine();
                Console.WriteLine("ggkimport.exe <autosomal-file> <ggk-db-path> <kit-no> <kit-name>");
                Console.WriteLine();
                return;
            }

            if (!File.Exists(args[0]))
            {
                Console.WriteLine(args[0] + " does not exist!");
                return;
            }
            if (!File.Exists(args[1]))
            {
                Console.WriteLine(args[1] + " does not exist!");
                return;
            }

            string file = args[0];
            SQLITE_DB = args[1];
            string kit_no = args[2];
            string name = args[3];

            try
            {                              
                importOpenSNPFile(kit_no, name, file);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);                
            }
            Console.WriteLine();
        }

        public static SQLiteConnection getDBConnection()
        {
            if (File.Exists(SQLITE_DB))
            {
                SQLiteConnection connection = new SQLiteConnection(@"Data Source=" + SQLITE_DB + @";Version=3; Compress=True; PRAGMA foreign_keys = ON; PRAGMA auto_vacuum = FULL;");
                connection.Open();
                Dictionary<string, string> pragma = new Dictionary<string, string>();

                pragma.Add("foreign_keys", "ON");
                pragma.Add("auto_vacuum", "FULL");
                SQLiteCommand ss2 = null;
                foreach (string key in pragma.Keys)
                {
                    ss2 = new SQLiteCommand("PRAGMA " + key + " = " + pragma[key] + ";", connection);
                    ss2.ExecuteNonQuery();
                }

                return connection;
            }
            else
            {
                Console.WriteLine("Data file ggk.db doesn't exist. ");
            }
            return null;
        }
        public static DataTable queryDatabase(string table, string[] fields, string conditions)
        {
            StringBuilder sb = new StringBuilder();
            foreach (string field in fields)
                sb.Append(field + " ");
            string fields_list = sb.ToString().Trim().Replace(' ', ',');
            SQLiteConnection conn = getDBConnection();
            SQLiteCommand ss = new SQLiteCommand("select " + fields_list + " from " + table + " " + conditions, conn);
            SQLiteDataReader reader = ss.ExecuteReader();
            DataTable dt = new DataTable(table);
            dt.Load(reader);
            reader.Close();
            conn.Close();
            return dt;
        }


        public static void importOpenSNPFile(string kit_no, string name, string filename)
        {
            Console.WriteLine("Importing: ----- "+filename+" -----");
            Console.WriteLine("Kit# " + kit_no + " [" + name + "]");
            StringBuilder sb = new StringBuilder();
            SQLiteConnection cnn = getDBConnection();
            try
            {
                DataTable dt2 = queryDatabase("kit_master", new string[] { "kit_no" }, "WHERE kit_no='" + kit_no + "'");
                if (dt2.Rows.Count == 0)
                {

                    //kit master
                    SQLiteCommand upCmd = new SQLiteCommand(@"INSERT OR REPLACE INTO kit_master(kit_no, name)values(@kit_no,@name)", cnn);
                    upCmd.Parameters.AddWithValue("@kit_no", kit_no);
                    upCmd.Parameters.AddWithValue("@name", name);
                    upCmd.ExecuteNonQuery();
                    upCmd.Dispose();


                    //kit autosomal
                    //upCmd = new SQLiteCommand(@"INSERT OR REPLACE INTO kit_autosomal(kit_no, rsid,chromosome,position,genotype)values(@kit_no,@rsid,@chromosome,@position,@genotype)", cnn);

                    DataTable dt = getDataTable(filename, kit_no);
                    Console.WriteLine("Autosomal DNA has " + dt.Rows.Count + " rows.");
                    int count = 0;
                    using (var transaction = cnn.BeginTransaction())
                    {
                        SQLiteDataAdapter sqliteAdapter = new SQLiteDataAdapter("SELECT kit_no,rsid,chromosome,position,genotype FROM kit_autosomal", cnn);
                        var cmdBuilder = new SQLiteCommandBuilder(sqliteAdapter);
                        count = sqliteAdapter.Update(dt);
                        Console.WriteLine("Inserted " + count + " rows.");
                        transaction.Commit();
                    }
                }
                else
                {
                    Console.WriteLine("Skipping Import: Kit already exists.");
                }
            }
            catch (Exception err)
            {
                Console.WriteLine("Not Saved. Techical Details: " + err.Message);
            }
            cnn.Dispose();
            Console.WriteLine("Task Completed.");
        }

        public static DataTable getDataTable(string file_path, string kit_no)
        {
            //kit_no,rsid,chromosome,position,genotype
            DataTable table = new DataTable();
            try
            {                
                table.Columns.Add("kit_no");
                table.Columns.Add("rsid");
                table.Columns.Add("chromosome");
                table.Columns.Add("position");
                table.Columns.Add("genotype");

                ArrayList rows = getAutosomalDNAList(file_path, kit_no);

                    foreach (string[] row in rows)
                    {
                        table.Rows.Add(row);
                    }

            }
            catch (Exception e)
            {
                // something. The most probable cause is user just exited. so, just cancel the job...
                Console.WriteLine("Error: "+e.Message);
            }
            return table;
        }

        private static int detectDNAFileType(string[] lines)
        {
            int count = 0;
            foreach (string line in lines)
            {
                if (line == "RSID,CHROMOSOME,POSITION,RESULT")
                    return AUTOSOMAL_FTDNA;
                if (line == "# rsid\tchromosome\tposition\tgenotype")
                    return AUTOSOMAL_23ANDME;
                if (line == "rsid\tchromosome\tposition\tallele1\tallele2")
                    return AUTOSOMAL_ANCESTRY;
                if (line == "Name,Variation,Chromosome,Position,Strand,YourCode")
                    return AUTOSOMAL_DECODEME;
                if (line == "SNP,Chr,Allele1,Allele2")
                    return AUTOSOMAL_GENO2;
                /* if above doesn't work */
                if (line.Split("\t".ToCharArray()).Length == 4)
                    return AUTOSOMAL_23ANDME;
                if (line.Split("\t".ToCharArray()).Length == 5)
                    return AUTOSOMAL_ANCESTRY;
                if (line.Split(",".ToCharArray()).Length == 4)
                    return AUTOSOMAL_FTDNA;
                if (line.Split(",".ToCharArray()).Length == 6)
                    return AUTOSOMAL_DECODEME;
                if (count > 100)
                {
                    // detection useless... 
                    break;
                }
                count++;
            }
            return -1;
        }

        private static string getPosition(string rsid)
        {
            return "0";
        }

        public static ArrayList getAutosomalDNAList(string file,string kit_no)
        {
            ArrayList rows = new ArrayList();
            string[] lines = null;
            lines = File.ReadAllLines(file);

            int type = detectDNAFileType(lines);

            if (type == -1)
            {
                Console.WriteLine("Unable to identify file format for " + file);
                return new ArrayList();
            }
            string[] data = null;
            string tLine = null;
            string rsid = null;
            string chr = null;
            string pos = null;
            string genotype = null;

            foreach (string line in lines)
            {
                //
                if (type == AUTOSOMAL_FTDNA)
                {
                    if (line.StartsWith("RSID"))
                        continue;
                    if (line.Trim() == "")
                        continue;
                    //
                    tLine = line.Replace("\"", "");
                    data = tLine.Split(",".ToCharArray());
                    rsid = data[0];
                    chr = data[1];
                    pos = data[2];
                    genotype = data[3];
                }
                if (type == AUTOSOMAL_23ANDME)
                {
                    if (line.StartsWith("#"))
                        continue;
                    if (line.Trim() == "")
                        continue;
                    //       
                    data = line.Split("\t".ToCharArray());
                    rsid = data[0];
                    chr = data[1];
                    pos = data[2];
                    genotype = data[3];
                }
                if (type == AUTOSOMAL_ANCESTRY)
                {
                    if (line.StartsWith("#"))
                        continue;
                    if (line.StartsWith("rsid\t"))
                        continue;
                    if (line.Trim() == "")
                        continue;
                    //            
                    data = line.Split("\t".ToCharArray());

                    rsid = data[0];
                    chr = data[1];
                    if (chr == "23")
                        chr = "X";
                    pos = data[2];
                    genotype = data[3] + data[4];
                }
                if (type == AUTOSOMAL_GENO2)
                {
                    if (line.StartsWith("SNP,"))
                        continue;
                    if (line.Trim() == "")
                        continue;
                    //            
                    data = line.Split(",".ToCharArray());

                    rsid = data[0];
                    chr = data[1];
                    pos = getPosition(rsid);
                    genotype = data[2] + data[3];
                }
                if (type == AUTOSOMAL_DECODEME)
                {
                    if (line.StartsWith("Name,"))
                        continue;
                    if (line.Trim() == "")
                        continue;
                    //            
                    data = line.Split(",".ToCharArray());

                    rsid = data[0];
                    chr = data[2];
                    pos = data[3];
                    genotype = data[5];
                }
                if (chr != "Y" && chr != "MT")
                {
                    if (chr != "0")
                        rows.Add(new string[] { kit_no, rsid, chr, pos, genotype });
                }
                else
                {
                    //                    
                }
            }
            return rows;
        }

    }
}
