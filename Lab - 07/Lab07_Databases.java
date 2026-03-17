/*
 * ===========================================================================
 *  Lab - 07: Handling and Analysing Databases using Cassandra, Hive & MongoDB
 * ===========================================================================
 *
 *  Course  : Big Data Analytics Lab
 *  Task    : CRUD and analysis across three NoSQL/Big-Data systems
 *  Language: Java
 *
 *  COMPILE:
 *    javac -cp \
 *      /usr/share/cassandra/lib/cassandra-driver-core-3.11.0-shaded.jar:\
 *      /usr/share/cassandra/lib/netty-all-4.1.58.Final.jar:\
 *      /usr/share/cassandra/lib/guava-27.0-jre.jar:\
 *      /usr/share/cassandra/lib/metrics-core-3.1.5.jar:\
 *      /usr/share/cassandra/lib/slf4j-api-1.7.25.jar:\
 *      ../Lab\ -\ 08/mongo-java-driver-3.12.14.jar \
 *      Lab07_Databases.java
 *
 *  RUN:
 *    java -Djava.util.logging.config.file=logging.properties \
 *      -cp .:/usr/share/cassandra/lib/cassandra-driver-core-3.11.0-shaded.jar:\
 *      /usr/share/cassandra/lib/netty-all-4.1.58.Final.jar:\
 *      /usr/share/cassandra/lib/guava-27.0-jre.jar:\
 *      /usr/share/cassandra/lib/metrics-core-3.1.5.jar:\
 *      /usr/share/cassandra/lib/slf4j-api-1.7.25.jar:\
 *      "../Lab - 08/mongo-java-driver-3.12.14.jar" \
 *      Lab07_Databases
 *
 *  PREREQUISITES:
 *    - Cassandra: sudo cassandra -R  (or service cassandra start)
 *    - MongoDB:   mongod --fork --logpath /tmp/mongod.log --dbpath ~/data/db
 *    - Hive:      Requires Hadoop HDFS (see lab07_hive.hql for HiveQL commands)
 *
 * ===========================================================================
 */

// ── Cassandra imports ───────────────────────────────────────
import com.datastax.driver.core.*;

// ── MongoDB imports ─────────────────────────────────────────
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.*;
import org.bson.Document;

import java.util.*;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Projections.*;

public class Lab07_Databases {

    // ── Width ────────────────────────────────────────────────
    static final int W = 65;

    // ── Safe number from MongoDB aggregation (Int or Long) ───
    static int num(Document d, String k) {
        Object v = d.get(k);
        return v instanceof Number ? ((Number) v).intValue() : 0;
    }
    static double dbl(Document d, String k) {
        Object v = d.get(k);
        return v instanceof Number ? ((Number) v).doubleValue() : 0.0;
    }

    // ── Display helpers ──────────────────────────────────────
    static void banner(String text) {
        System.out.println();
        System.out.println(rep("=", W));
        int lp = (W - text.length() - 4) / 2;
        int rp = W - lp - text.length() - 4;
        System.out.println("| " + rep(" ", lp) + text + rep(" ", rp) + " |");
        System.out.println(rep("=", W));
    }

    static void partBanner(String part, String title) {
        System.out.println();
        System.out.println(rep("#", W));
        System.out.println("##  " + part + " - " + title);
        System.out.println(rep("#", W));
    }

    static void header(int step, String title) {
        System.out.println();
        System.out.println(rep("-", W));
        System.out.println("  STEP " + step + " - " + title);
        System.out.println(rep("-", W));
    }

    static void sub(String title) {
        System.out.println();
        System.out.println("  >>  " + title);
        System.out.println("  " + rep(".", W - 4));
    }

    static void ok(String m)   { System.out.println("  [OK]  " + m); }
    static void info(String m) { System.out.println("  -->   " + m); }
    static void note(String m) { System.out.println("  [!]   " + m); }

    static String rep(String s, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append(s);
        return sb.toString();
    }

    static String pr(Object o, int len) {
        String s = String.valueOf(o);
        if (s.length() >= len) return s.substring(0, len);
        return s + rep(" ", len - s.length());
    }

    // ── Table printer ────────────────────────────────────────
    static void div(int[] cols) {
        StringBuilder sb = new StringBuilder("  +");
        for (int c : cols) sb.append(rep("-", c + 2)).append("+");
        System.out.println(sb);
    }

    static void row(Object[] vals, int[] cols) {
        StringBuilder sb = new StringBuilder("  |");
        for (int i = 0; i < vals.length; i++)
            sb.append(" ").append(pr(vals[i], cols[i])).append(" |");
        System.out.println(sb);
    }

    static void table(String[] hdr, List<Object[]> rows, int[] cols) {
        div(cols);
        row(hdr, cols);
        div(cols);
        for (Object[] r : rows) row(r, cols);
        div(cols);
    }

    // ── Shared student dataset ───────────────────────────────
    static final Object[][] STUDENTS = {
        {1,  "Amit Kumar",  "CSE", 85, "Delhi",     2},
        {2,  "Neha Sharma", "IT",  90, "Mumbai",    3},
        {3,  "Ravi Verma",  "CSE", 72, "Chennai",   1},
        {4,  "Priya Singh", "ECE", 88, "Pune",      2},
        {5,  "Arjun Patel", "CSE", 94, "Ahmedabad", 3},
        {6,  "Sneha Gupta", "IT",  76, "Delhi",     2},
        {7,  "Karan Mehta", "ECE", 63, "Mumbai",    1},
        {8,  "Divya Joshi", "CSE", 91, "Jaipur",    3},
        {9,  "Rohit Das",   "IT",  55, "Kolkata",   1},
        {10, "Meera Nair",  "ECE", 82, "Kochi",     2},
    };

    // ═══════════════════════════════════════════════════════
    //  MAIN
    // ═══════════════════════════════════════════════════════
    public static void main(String[] args) {

        banner("Lab 07 - Cassandra | Hive | MongoDB");
        System.out.println("  Database  : student_db");
        System.out.println("  Dataset   : 10 students (CSE / IT / ECE)");
        System.out.println("  Parts     : A=Cassandra  B=Hive  C=MongoDB");

        runCassandra();
        runHive();
        runMongoDB();

        banner("Lab 07 - Completed Successfully");
        System.out.println("  Cassandra: CRUD + aggregation queries executed");
        System.out.println("  Hive     : HiveQL commands displayed (needs Hadoop)");
        System.out.println("  MongoDB  : CRUD + aggregation pipeline executed");
        System.out.println();
    }

    // ═══════════════════════════════════════════════════════
    //  PART A: CASSANDRA
    // ═══════════════════════════════════════════════════════
    static void runCassandra() {
        partBanner("PART A", "Apache Cassandra (Column-Family NoSQL)");

        Cluster cluster = null;
        Session session = null;
        try {
            cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withPort(9042)
                .build();
            session = cluster.connect();
            ok("Connected to Cassandra at 127.0.0.1:9042");
            String v = cluster.getMetadata().getClusterName();
            info("Cluster name: " + v);

            // ── Step 1: Keyspace ─────────────────────────────
            header(1, "CREATE KEYSPACE & TABLE");
            session.execute(
                "DROP KEYSPACE IF EXISTS student_db");
            session.execute(
                "CREATE KEYSPACE student_db " +
                "WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            session.execute("USE student_db");
            ok("Keyspace 'student_db' created");

            session.execute(
                "CREATE TABLE students (" +
                "id INT PRIMARY KEY, name TEXT, department TEXT, marks INT)");
            ok("Table 'students' created");

            // ── Step 2: Insert ───────────────────────────────
            header(2, "INSERT ROWS");
            PreparedStatement ps = session.prepare(
                "INSERT INTO students (id, name, department, marks) VALUES (?, ?, ?, ?)");
            for (Object[] s : STUDENTS)
                session.execute(ps.bind(s[0], (String) s[1], (String) s[2], s[3]));
            ok("Inserted " + STUDENTS.length + " rows");

            // ── Step 3: Retrieve all ─────────────────────────
            header(3, "RETRIEVE ALL ROWS");
            sub("SELECT * FROM students");
            ResultSet rs = session.execute("SELECT id, name, department, marks FROM students");
            List<Row> allRows = rs.all();
            // Sort by id client-side (Cassandra returns by partition token)
            allRows.sort(Comparator.comparingInt(r -> r.getInt("id")));
            List<Object[]> tRows = new ArrayList<>();
            for (Row r : allRows)
                tRows.add(new Object[]{r.getInt("id"), r.getString("name"), r.getString("department"), r.getInt("marks")});
            table(new String[]{"ID", "Name", "Dept", "Marks"}, tRows, new int[]{3, 14, 5, 5});
            info("Total rows: " + allRows.size());

            // ── Step 4: Filtered queries ─────────────────────
            header(4, "FILTERED & ANALYSIS QUERIES");

            sub("Students with marks > 80  (ALLOW FILTERING)");
            ResultSet rs2 = session.execute(
                "SELECT name, department, marks FROM students WHERE marks > 80 ALLOW FILTERING");
            List<Row> filtered = rs2.all();
            filtered.sort((a, b) -> b.getInt("marks") - a.getInt("marks"));
            List<Object[]> fRows = new ArrayList<>();
            for (Row r : filtered)
                fRows.add(new Object[]{r.getString("name"), r.getString("department"), r.getInt("marks")});
            table(new String[]{"Name", "Dept", "Marks"}, fRows, new int[]{14, 5, 5});
            info("Count: " + filtered.size() + " students scored above 80");

            sub("Average, Min, Max and Count of all students");
            Row agg = session.execute(
                "SELECT AVG(marks) AS avg_m, MIN(marks) AS min_m, MAX(marks) AS max_m, COUNT(*) AS cnt FROM students"
            ).one();
            // Note: Cassandra AVG on INT column returns INT (truncated), not FLOAT
            System.out.println("     Average Marks : " + agg.getInt("avg_m"));
            System.out.println("     Min Marks     : " + agg.getInt("min_m"));
            System.out.println("     Max Marks     : " + agg.getInt("max_m"));
            System.out.println("     Total Students: " + agg.getLong("cnt"));

            sub("Students in CSE department  (after CREATE INDEX)");
            session.execute("CREATE INDEX IF NOT EXISTS ON students (department)");
            ResultSet rs3 = session.execute(
                "SELECT name, marks FROM students WHERE department = 'CSE'");
            for (Row r : rs3)
                System.out.println("     " + pr(r.getString("name"), 14) + " : " + r.getInt("marks"));

            // ── Step 5: Update ───────────────────────────────
            header(5, "UPDATE ROW");
            Row before = session.execute(
                "SELECT name, marks FROM students WHERE id = 3").one();
            if (before != null)
                info("BEFORE : " + before.getString("name") + " -> Marks: " + before.getInt("marks"));

            session.execute("UPDATE students SET marks = 80 WHERE id = 3");
            ok("Updated id=3 marks to 80");

            Row after = session.execute(
                "SELECT name, marks FROM students WHERE id = 3").one();
            if (after != null)
                info("AFTER  : " + after.getString("name") + " -> Marks: " + after.getInt("marks") + "  (was 72)");

            // ── Step 6: Delete ───────────────────────────────
            header(6, "DELETE ROW");
            info("Count before delete: " +
                session.execute("SELECT COUNT(*) FROM students").one().getLong(0));
            session.execute("DELETE FROM students WHERE id = 7");
            ok("Deleted student with id=7 (Karan Mehta)");
            info("Count after delete : " +
                session.execute("SELECT COUNT(*) FROM students").one().getLong(0));

            // ── Cleanup ──────────────────────────────────────
            session.execute("DROP KEYSPACE student_db");
            ok("Keyspace dropped. Cassandra session closed.");

        } catch (Exception e) {
            System.out.println("\n  [ERROR] Cassandra: " + e.getMessage());
            System.out.println("  Ensure Cassandra is running: sudo cassandra -R");
        } finally {
            if (session != null) session.close();
            if (cluster != null) cluster.close();
        }
    }

    // ═══════════════════════════════════════════════════════
    //  PART B: HIVE  (requires Hadoop — displayed as HiveQL)
    // ═══════════════════════════════════════════════════════
    static void runHive() {
        partBanner("PART B", "Apache Hive (SQL-on-Hadoop Data Warehouse)");

        note("Hive requires Hadoop HDFS to be running.");
        note("Start Hadoop:  start-dfs.sh && start-yarn.sh");
        note("Then run HiveQL commands:  hive -f lab07_hive.hql");
        note("HiveQL commands are shown below for reference:");

        header(1, "CREATE DATABASE & TABLE");
        hql("CREATE DATABASE IF NOT EXISTS student_db;");
        hql("USE student_db;");
        hql("CREATE TABLE IF NOT EXISTS students (");
        hql("    id INT, name STRING, department STRING, marks INT");
        hql(") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;");
        sub("Expected Output");
        System.out.println("     OK");
        System.out.println("     Time taken: 0.x seconds");

        header(2, "LOAD DATA & RETRIEVE");
        hql("LOAD DATA LOCAL INPATH '/tmp/students.csv' INTO TABLE students;");
        hql("SELECT * FROM students;");
        sub("Expected Output");
        int[] hw = {3, 14, 5, 5};
        List<Object[]> hRows = new ArrayList<>();
        for (Object[] s : STUDENTS)
            hRows.add(new Object[]{s[0], s[1], s[2], s[3]});
        table(new String[]{"ID", "Name", "Dept", "Mark"}, hRows, hw);

        header(3, "ANALYSIS QUERIES");
        sub("Students with marks > 80");
        hql("SELECT * FROM students WHERE marks > 80;");
        List<Object[]> h2 = new ArrayList<>();
        for (Object[] s : STUDENTS)
            if ((int) s[3] > 80) h2.add(new Object[]{s[1], s[2], s[3]});
        table(new String[]{"Name", "Dept", "Marks"}, h2, new int[]{14, 5, 5});

        sub("Average marks per department  (GROUP BY)");
        hql("SELECT department, AVG(marks) AS avg_marks");
        hql("FROM students GROUP BY department ORDER BY avg_marks DESC;");
        List<Object[]> h3 = new ArrayList<>();
        h3.add(new Object[]{"CSE", "85.50"}); h3.add(new Object[]{"ECE", "77.67"}); h3.add(new Object[]{"IT", "73.67"});
        table(new String[]{"Department", "Avg Marks"}, h3, new int[]{12, 10});

        sub("Count per department");
        hql("SELECT department, COUNT(*) AS student_count FROM students GROUP BY department;");
        List<Object[]> h4 = new ArrayList<>();
        h4.add(new Object[]{"CSE", 4}); h4.add(new Object[]{"ECE", 3}); h4.add(new Object[]{"IT", 3});
        table(new String[]{"Department", "Count"}, h4, new int[]{12, 7});

        sub("Top scorer");
        hql("SELECT * FROM students ORDER BY marks DESC LIMIT 1;");
        System.out.println("     Arjun Patel  | CSE | 94");

        ok("Hive HiveQL commands displayed. Run lab07_hive.hql when Hadoop is active.");
    }

    static void hql(String sql) {
        System.out.println("     hive> " + sql);
    }

    // ═══════════════════════════════════════════════════════
    //  PART C: MONGODB
    // ═══════════════════════════════════════════════════════
    static void runMongoDB() {
        partBanner("PART C", "MongoDB (Document-Oriented NoSQL)");

        MongoClient client = null;
        try {
            client = MongoClients.create("mongodb://localhost:27017");
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            ok("Connected to MongoDB on localhost:27017");
        } catch (Exception e) {
            System.out.println("\n  [ERROR] MongoDB: " + e.getMessage());
            System.out.println("  Start MongoDB: mongod --fork --logpath /tmp/mongod.log --dbpath ~/data/db");
            if (client != null) client.close();
            return;
        }

        MongoDatabase db = client.getDatabase("student_db");
        MongoCollection<Document> col = db.getCollection("students");
        col.drop();
        info("Cleared previous collection");

        // ── Step 1: Insert ───────────────────────────────────
        header(1, "INSERT DOCUMENTS");
        List<Document> docs = new ArrayList<>();
        for (Object[] s : STUDENTS)
            docs.add(new Document("id", s[0]).append("name", s[1])
                .append("department", s[2]).append("marks", s[3])
                .append("city", s[4]).append("year", s[5]));
        col.insertMany(docs);
        ok("Inserted " + STUDENTS.length + " documents via insertMany");
        ok("Total documents: " + col.countDocuments());

        // ── Step 2: Retrieve ─────────────────────────────────
        header(2, "RETRIEVE ALL DOCUMENTS");
        sub("find() - all students");
        List<Object[]> mRows = new ArrayList<>();
        for (Document d : col.find().sort(ascending("id")))
            mRows.add(new Object[]{d.getInteger("id"), d.getString("name"),
                d.getString("department"), d.getInteger("marks"), d.getString("city")});
        table(new String[]{"ID", "Name", "Dept", "Mark", "City"},
              mRows, new int[]{3, 14, 5, 4, 10});

        // ── Step 3: Filter queries ───────────────────────────
        header(3, "FILTER QUERIES");

        sub("Marks > 80  ($gt)");
        List<Object[]> f1 = new ArrayList<>();
        for (Document d : col.find(gt("marks", 80))
                .projection(fields(include("name", "department", "marks"), excludeId()))
                .sort(descending("marks")))
            f1.add(new Object[]{d.getString("name"), d.getString("department"), d.getInteger("marks")});
        table(new String[]{"Name", "Dept", "Marks"}, f1, new int[]{14, 5, 5});
        info("Count: " + f1.size() + " students above 80");

        sub("CSE AND marks > 80");
        for (Document d : col.find(and(eq("department", "CSE"), gt("marks", 80)))
                .projection(fields(include("name", "marks"), excludeId())))
            System.out.println("     " + pr(d.getString("name"), 14) + " : " + d.getInteger("marks"));

        // ── Step 4: Aggregation ──────────────────────────────
        header(4, "AGGREGATION PIPELINE");

        sub("Overall statistics ($group on null)");
        Document ov = col.aggregate(Collections.singletonList(
            group(null, sum("total", 1), avg("avgM", "$marks"),
                  max("maxM", "$marks"), min("minM", "$marks"))
        )).first();
        if (ov != null) {
            System.out.println("     Total Students : " + num(ov, "total"));
            System.out.printf ("     Average Marks  : %.2f%n", dbl(ov, "avgM"));
            System.out.println("     Highest Marks  : " + num(ov, "maxM"));
            System.out.println("     Lowest Marks   : " + num(ov, "minM"));
        }

        sub("Department-wise stats ($group by department)");
        List<Object[]> dRows = new ArrayList<>();
        for (Document d : col.aggregate(Arrays.asList(
                group("$department", sum("cnt", 1), avg("avgM", "$marks"),
                      max("maxM", "$marks"), min("minM", "$marks")),
                sort(descending("avgM")))))
            dRows.add(new Object[]{d.getString("_id"), num(d,"cnt"),
                String.format("%.2f", dbl(d,"avgM")), num(d,"maxM"), num(d,"minM")});
        table(new String[]{"Dept","Count","Avg","Max","Min"}, dRows, new int[]{5,5,7,4,4});

        sub("Rank list - sorted by marks (descending)");
        int r = 1;
        for (Document d : col.find()
                .projection(fields(include("name", "department", "marks"), excludeId()))
                .sort(descending("marks")))
            System.out.println("     #" + pr(r++, 2) + " " +
                pr(d.getString("name"), 14) + " | " + pr(d.getString("department"), 4) +
                " | " + d.getInteger("marks"));

        // ── Step 5: Update ───────────────────────────────────
        header(5, "UPDATE DOCUMENTS");

        sub("updateOne - Ravi Verma marks: 72 -> 80");
        Document bef = col.find(eq("id", 3)).first();
        if (bef != null) info("BEFORE: " + bef.getString("name") + " -> " + bef.getInteger("marks"));
        UpdateResult u1 = col.updateOne(eq("id", 3), Updates.set("marks", 80));
        ok("matchedCount: " + u1.getMatchedCount() + "  modifiedCount: " + u1.getModifiedCount());
        Document aft = col.find(eq("id", 3)).first();
        if (aft != null) info("AFTER : " + aft.getString("name") + " -> " + aft.getInteger("marks"));

        sub("updateMany - +5 bonus marks to all IT students");
        info("IT students BEFORE:");
        for (Document d : col.find(eq("department", "IT"))
                .projection(fields(include("name", "marks"), excludeId())))
            System.out.println("       " + pr(d.getString("name"), 14) + ": " + d.getInteger("marks"));
        UpdateResult u2 = col.updateMany(eq("department", "IT"), Updates.inc("marks", 5));
        ok("modifiedCount: " + u2.getModifiedCount());
        info("IT students AFTER (+5):");
        for (Document d : col.find(eq("department", "IT"))
                .projection(fields(include("name", "marks"), excludeId())))
            System.out.println("       " + pr(d.getString("name"), 14) + ": " + d.getInteger("marks"));

        // ── Step 6: Delete ───────────────────────────────────
        header(6, "DELETE DOCUMENTS");
        info("Count before: " + col.countDocuments());
        DeleteResult del = col.deleteOne(eq("id", 9));
        ok("deleteOne (id=9 Rohit Das) -> deletedCount: " + del.getDeletedCount());
        info("Count after : " + col.countDocuments());

        // ── Final state ──────────────────────────────────────
        banner("PART C - FINAL STATE (MongoDB)");
        List<Object[]> fin = new ArrayList<>();
        for (Document d : col.find()
                .projection(fields(include("id","name","department","marks"), excludeId()))
                .sort(descending("marks")))
            fin.add(new Object[]{d.getInteger("id"), d.getString("name"),
                d.getString("department"), d.getInteger("marks")});
        table(new String[]{"ID","Name","Dept","Marks"}, fin, new int[]{3,14,5,5});
        System.out.println("\n  Remaining documents: " + col.countDocuments());

        col.drop();
        client.close();
        ok("Collection dropped. MongoDB connection closed.");
    }
}
