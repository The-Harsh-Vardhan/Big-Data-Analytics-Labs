/*
 * ===========================================================================
 *  Lab - 08: Handling and Analysing Database using MongoDB (NoSQL)
 * ===========================================================================
 *
 *  Course  : Big Data Analytics Lab
 *  Task    : Perform CRUD operations and data analysis using MongoDB
 *  Language: Java (MongoDB Java Driver 3.12)
 *
 *  COMPILE & RUN:
 *  ---------------------------------------------------------------------------
 *  Step 1 – Download the driver:
 *    wget https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.14/mongo-java-driver-3.12.14.jar
 *
 *  Step 2 – Compile:
 *    javac -cp mongo-java-driver-3.12.14.jar Lab08_MongoDB.java
 *
 *  Step 3 – Run:
 *    java -cp .:mongo-java-driver-3.12.14.jar Lab08_MongoDB
 *
 *  PREREQUISITE – MongoDB must be running:
 *    mongod --fork --logpath /var/log/mongod.log --dbpath /data/db
 *
 * ===========================================================================
 */

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

public class Lab08_MongoDB {

    // ── Constants ───────────────────────────────────────────
    static final int W = 65;

    // ── Safe number extraction from aggregation results ─────
    // $sum / $count in aggregation may return Integer OR Long.
    // This helper handles both so we never get null.
    static int num(Document d, String key) {
        Object v = d.get(key);
        if (v instanceof Number) return ((Number) v).intValue();
        return 0;
    }

    static double dbl(Document d, String key) {
        Object v = d.get(key);
        if (v instanceof Number) return ((Number) v).doubleValue();
        return 0.0;
    }

    // ── Display helpers ─────────────────────────────────────
    static void banner(String text) {
        System.out.println();
        System.out.println(rep("=", W));
        int pad = (W - text.length() - 4) / 2;
        int rpad = W - pad - text.length() - 4;
        System.out.println("| " + rep(" ", pad) + text + rep(" ", rpad) + " |");
        System.out.println(rep("=", W));
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

    static void ok(String msg)   { System.out.println("  [OK]  " + msg); }
    static void info(String msg) { System.out.println("  -->   " + msg); }

    static String rep(String s, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append(s);
        return sb.toString();
    }

    static String pr(Object o, int len) {   // pad right
        String s = String.valueOf(o);
        if (s.length() >= len) return s.substring(0, len);
        return s + rep(" ", len - s.length());
    }

    // ── Table printing ──────────────────────────────────────
    static void divider(int[] cols) {
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
        divider(cols);
        row(hdr, cols);
        divider(cols);
        for (Object[] r : rows) row(r, cols);
        divider(cols);
    }

    // ── Dataset ─────────────────────────────────────────────
    static List<Document> dataset() {
        String[][] data = {
            {"1",  "Amit Kumar",  "CSE", "85", "Delhi",     "2"},
            {"2",  "Neha Sharma", "IT",  "90", "Mumbai",    "3"},
            {"3",  "Ravi Verma",  "CSE", "72", "Chennai",   "1"},
            {"4",  "Priya Singh", "ECE", "88", "Pune",      "2"},
            {"5",  "Arjun Patel", "CSE", "94", "Ahmedabad", "3"},
            {"6",  "Sneha Gupta", "IT",  "76", "Delhi",     "2"},
            {"7",  "Karan Mehta", "ECE", "63", "Mumbai",    "1"},
            {"8",  "Divya Joshi", "CSE", "91", "Jaipur",    "3"},
            {"9",  "Rohit Das",   "IT",  "55", "Kolkata",   "1"},
            {"10", "Meera Nair",  "ECE", "82", "Kochi",     "2"}
        };
        List<Document> list = new ArrayList<>();
        for (String[] r : data)
            list.add(new Document("id",         Integer.parseInt(r[0]))
                        .append("name",         r[1])
                        .append("department",   r[2])
                        .append("marks",        Integer.parseInt(r[3]))
                        .append("city",         r[4])
                        .append("year",         Integer.parseInt(r[5])));
        return list;
    }

    // ═══════════════════════════════════════════════════════
    //  MAIN
    // ═══════════════════════════════════════════════════════
    public static void main(String[] args) {

        banner("Lab 08 - MongoDB CRUD & Data Analysis");
        System.out.println("  Database  : student_db");
        System.out.println("  Collection: students");
        System.out.println("  Records   : 10 students");
        System.out.println("  Driver    : MongoDB Java Driver 3.12");

        // ── Connect ─────────────────────────────────────────
        MongoClient client;
        try {
            client = MongoClients.create("mongodb://localhost:27017");
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            ok("Connected to MongoDB on localhost:27017");
        } catch (Exception e) {
            System.out.println("\n  [ERROR] Cannot connect to MongoDB: " + e.getMessage());
            System.out.println("  Start MongoDB with:");
            System.out.println("    mongod --fork --logpath /var/log/mongod.log --dbpath /data/db\n");
            return;
        }

        MongoDatabase db  = client.getDatabase("student_db");
        MongoCollection<Document> col = db.getCollection("students");
        col.drop();
        info("Dropped previous collection (if existed)");

        // ────────────────────────────────────────────────────
        //  STEP 1: INSERT
        // ────────────────────────────────────────────────────
        header(1, "INSERT DOCUMENTS");
        List<Document> students = dataset();

        sub("insertOne - first student");
        col.insertOne(students.get(0));
        ok("Inserted: " + students.get(0).getString("name"));

        sub("insertMany - remaining 9 students");
        col.insertMany(students.subList(1, students.size()));
        ok("Inserted 9 more documents");
        ok("Total documents now: " + col.countDocuments());

        // ────────────────────────────────────────────────────
        //  STEP 2: RETRIEVE
        // ────────────────────────────────────────────────────
        header(2, "RETRIEVE ALL DOCUMENTS");

        sub("All students - find()");
        int[] w2 = {3, 14, 5, 5, 10, 4};
        List<Object[]> rows = new ArrayList<>();
        for (Document d : col.find().sort(ascending("id")))
            rows.add(new Object[]{d.getInteger("id"), d.getString("name"),
                d.getString("department"), d.getInteger("marks"),
                d.getString("city"), d.getInteger("year")});
        table(new String[]{"ID", "Name", "Dept", "Mark", "City", "Yr"}, rows, w2);
        info("Total: " + col.countDocuments() + " documents");

        sub("findOne - search by name: Arjun Patel");
        Document arjun = col.find(eq("name", "Arjun Patel")).first();
        if (arjun != null) {
            System.out.println("     Name       : " + arjun.getString("name"));
            System.out.println("     Department : " + arjun.getString("department"));
            System.out.println("     Marks      : " + arjun.getInteger("marks"));
            System.out.println("     City       : " + arjun.getString("city"));
            System.out.println("     Year       : " + arjun.getInteger("year"));
        }

        sub("Find all CSE students");
        for (Document d : col.find(eq("department", "CSE"))
                .projection(fields(include("name", "marks"), excludeId())))
            System.out.println("     " + pr(d.getString("name"), 14) + " : " + d.getInteger("marks"));

        // ────────────────────────────────────────────────────
        //  STEP 3: FILTER QUERIES
        // ────────────────────────────────────────────────────
        header(3, "FILTERED QUERIES");

        sub("Marks > 80  ($gt operator)");
        List<Object[]> f1 = new ArrayList<>();
        for (Document d : col.find(gt("marks", 80))
                .projection(fields(include("name", "department", "marks"), excludeId()))
                .sort(descending("marks")))
            f1.add(new Object[]{d.getString("name"), d.getString("department"), d.getInteger("marks")});
        table(new String[]{"Name", "Dept", "Marks"}, f1, new int[]{14, 5, 5});
        info("Count: " + f1.size() + " students scored above 80");

        sub("Marks < 70  (at-risk students, $lt operator)");
        List<Object[]> f2 = new ArrayList<>();
        for (Document d : col.find(lt("marks", 70))
                .projection(fields(include("name", "department", "marks"), excludeId())))
            f2.add(new Object[]{d.getString("name"), d.getString("department"), d.getInteger("marks")});
        table(new String[]{"Name", "Dept", "Marks"}, f2, new int[]{14, 5, 5});
        info("Count: " + f2.size() + " at-risk student(s)");

        sub("CSE students with marks > 80  (AND filter)");
        for (Document d : col.find(and(eq("department", "CSE"), gt("marks", 80)))
                .projection(fields(include("name", "marks"), excludeId())))
            System.out.println("     " + pr(d.getString("name"), 14) + " : " + d.getInteger("marks"));

        sub("CSE or ECE students  ($or operator)");
        List<Object[]> f3 = new ArrayList<>();
        for (Document d : col.find(or(eq("department", "CSE"), eq("department", "ECE")))
                .projection(fields(include("name", "department", "marks"), excludeId()))
                .sort(ascending("department")))
            f3.add(new Object[]{d.getString("name"), d.getString("department"), d.getInteger("marks")});
        table(new String[]{"Name", "Dept", "Marks"}, f3, new int[]{14, 5, 5});

        sub("CSE or IT students  ($in operator)");
        for (Document d : col.find(in("department", "CSE", "IT"))
                .projection(fields(include("name", "department"), excludeId())))
            System.out.println("     " + pr(d.getString("name"), 14) + " | " + d.getString("department"));

        // ────────────────────────────────────────────────────
        //  STEP 4: SORT & RANK
        // ────────────────────────────────────────────────────
        header(4, "SORTING & RANK LIST");

        sub("All students sorted by marks (descending)");
        List<Object[]> rank = new ArrayList<>();
        int r = 1;
        for (Document d : col.find()
                .projection(fields(include("name", "department", "marks"), excludeId()))
                .sort(descending("marks")))
            rank.add(new Object[]{r++, d.getString("name"), d.getString("department"), d.getInteger("marks")});
        table(new String[]{"Rk", "Name", "Dept", "Marks"}, rank, new int[]{2, 14, 5, 5});

        sub("Top 3 performers");
        int pos = 1;
        for (Document d : col.find()
                .projection(fields(include("name", "marks"), excludeId()))
                .sort(descending("marks")).limit(3))
            System.out.println("     #" + pos++ + "  " + pr(d.getString("name"), 14) + " - " + d.getInteger("marks") + "/100");

        sub("Bottom 3 performers");
        pos = 1;
        for (Document d : col.find()
                .projection(fields(include("name", "marks"), excludeId()))
                .sort(ascending("marks")).limit(3))
            System.out.println("     #" + pos++ + "  " + pr(d.getString("name"), 14) + " - " + d.getInteger("marks") + "/100");

        // ────────────────────────────────────────────────────
        //  STEP 5: AGGREGATION PIPELINE
        // ────────────────────────────────────────────────────
        header(5, "DATA ANALYSIS - AGGREGATION PIPELINE");

        // 5.1 Overall stats
        sub("5.1  Overall Statistics");
        Document ov = col.aggregate(Collections.singletonList(
            group(null,
                sum("total",    1),
                avg("avgM",    "$marks"),
                max("maxM",    "$marks"),
                min("minM",    "$marks"),
                sum("sumM",    "$marks"))
        )).first();
        if (ov != null) {
            System.out.println("     Total Students : " + num(ov, "total"));
            System.out.printf ("     Average Marks  : %.2f%n", dbl(ov, "avgM"));
            System.out.println("     Highest Marks  : " + num(ov, "maxM"));
            System.out.println("     Lowest Marks   : " + num(ov, "minM"));
            System.out.println("     Total Marks    : " + num(ov, "sumM"));
        }

        // 5.2 Department-wise
        sub("5.2  Department-wise Statistics");
        List<Object[]> deptRows = new ArrayList<>();
        for (Document d : col.aggregate(Arrays.asList(
                group("$department",
                    sum("cnt", 1),
                    avg("avgM", "$marks"),
                    max("maxM", "$marks"),
                    min("minM", "$marks")),
                sort(descending("avgM"))
        )))
            deptRows.add(new Object[]{d.getString("_id"), num(d,"cnt"),
                String.format("%.2f", dbl(d,"avgM")), num(d,"maxM"), num(d,"minM")});
        table(new String[]{"Dept", "Count", "Avg", "Max", "Min"}, deptRows,
              new int[]{5, 5, 7, 4, 4});

        // 5.3 Depts where avg > 75
        sub("5.3  Departments with Average Marks > 75");
        for (Document d : col.aggregate(Arrays.asList(
                group("$department", avg("avgM", "$marks")),
                match(gt("avgM", 75.0)),
                sort(descending("avgM"))
        ))) {
            double avg = dbl(d, "avgM");
            System.out.printf("     %-5s  %5.2f  %s%n",
                d.getString("_id"), avg, rep("#", (int)(avg / 5)));
        }

        // 5.4 Year-wise breakdown
        sub("5.4  Students per Year of Study");
        for (Document d : col.aggregate(Arrays.asList(
                group("$year",
                    sum("cnt", 1),
                    push("names", "$name")),
                sort(ascending("_id"))
        ))) {
            @SuppressWarnings("unchecked")
            List<String> names = (List<String>) d.get("names");
            System.out.println("     Year " + num(d, "_id") +
                " (" + num(d, "cnt") + " students): " + String.join(", ", names));
        }

        // 5.5 City-wise
        sub("5.5  City-wise Student Count");
        for (Document d : col.aggregate(Arrays.asList(
                group("$city", sum("cnt", 1)),
                sort(descending("cnt"))
        )))
            System.out.println("     " + pr(d.getString("_id"), 12) +
                " : " + rep("*", num(d,"cnt")) + " (" + num(d,"cnt") + ")");

        // ────────────────────────────────────────────────────
        //  STEP 6: UPDATE
        // ────────────────────────────────────────────────────
        header(6, "UPDATE DOCUMENTS");

        sub("6.1  updateOne - Ravi Verma marks: 72 -> 78");
        Document bef = col.find(eq("id", 3)).first();
        if (bef != null)
            info("BEFORE : " + bef.getString("name") + " -> Marks: " + bef.getInteger("marks"));
        UpdateResult u1 = col.updateOne(eq("id", 3), Updates.set("marks", 78));
        ok("matchedCount: " + u1.getMatchedCount() + ", modifiedCount: " + u1.getModifiedCount());
        Document aft = col.find(eq("id", 3)).first();
        if (aft != null)
            info("AFTER  : " + aft.getString("name") + " -> Marks: " + aft.getInteger("marks") + "  (+6 marks)");

        sub("6.2  updateMany - Bonus +5 marks for all IT students");
        info("IT students BEFORE bonus:");
        for (Document d : col.find(eq("department", "IT"))
                .projection(fields(include("name", "marks"), excludeId())))
            System.out.println("       " + pr(d.getString("name"), 14) + ": " + d.getInteger("marks"));

        UpdateResult u2 = col.updateMany(eq("department", "IT"), Updates.inc("marks", 5));
        ok("matchedCount: " + u2.getMatchedCount() + "  modifiedCount: " + u2.getModifiedCount());

        info("IT students AFTER bonus (+5):");
        for (Document d : col.find(eq("department", "IT"))
                .projection(fields(include("name", "marks"), excludeId())))
            System.out.println("       " + pr(d.getString("name"), 14) + ": " + d.getInteger("marks"));

        sub("6.3  Add grade field using updateMany + $set");
        col.updateMany(gte("marks", 90),                          Updates.set("grade", "A"));
        col.updateMany(and(gte("marks", 75), lt("marks", 90)),    Updates.set("grade", "B"));
        col.updateMany(and(gte("marks", 60), lt("marks", 75)),    Updates.set("grade", "C"));
        col.updateMany(lt("marks", 60),                           Updates.set("grade", "D"));
        ok("Grade field assigned to all documents");

        List<Object[]> gradeRows = new ArrayList<>();
        for (Document d : col.find()
                .projection(fields(include("name", "department", "marks", "grade"), excludeId()))
                .sort(descending("marks")))
            gradeRows.add(new Object[]{d.getString("name"), d.getString("department"),
                d.getInteger("marks"), d.getString("grade") != null ? d.getString("grade") : "?"});
        table(new String[]{"Name", "Dept", "Marks", "Grade"}, gradeRows, new int[]{14, 5, 5, 5});

        // ────────────────────────────────────────────────────
        //  STEP 7: DELETE
        // ────────────────────────────────────────────────────
        header(7, "DELETE DOCUMENTS");
        info("Total before delete: " + col.countDocuments());

        sub("7.1  deleteOne - Remove Rohit Das (id: 9)");
        DeleteResult d1 = col.deleteOne(eq("id", 9));
        ok("deletedCount: " + d1.getDeletedCount());
        info("Find Rohit Das after delete: " + col.find(eq("id", 9)).first());
        info("Total after deleteOne: " + col.countDocuments());

        sub("7.2  deleteMany - Remove all grade D students");
        DeleteResult d2 = col.deleteMany(eq("grade", "D"));
        ok("deletedCount: " + d2.getDeletedCount());
        info("Total after deleteMany: " + col.countDocuments());

        // ────────────────────────────────────────────────────
        //  STEP 8: INDEXING
        // ────────────────────────────────────────────────────
        header(8, "INDEXING");

        sub("8.1  Single-field index on department");
        ok("Created: " + col.createIndex(Indexes.ascending("department")));

        sub("8.2  Compound index (department ASC, marks DESC)");
        ok("Created: " + col.createIndex(Indexes.compoundIndex(
            Indexes.ascending("department"), Indexes.descending("marks"))));

        sub("8.3  List all indexes");
        for (Document idx : col.listIndexes())
            System.out.println("     " + pr(idx.getString("name"), 30) + " -> key: " + idx.get("key"));

        // ────────────────────────────────────────────────────
        //  FINAL STATE
        // ────────────────────────────────────────────────────
        banner("FINAL STATE - Remaining Documents");
        List<Object[]> fin = new ArrayList<>();
        for (Document d : col.find()
                .projection(fields(include("id","name","department","marks","grade","city"), excludeId()))
                .sort(descending("marks")))
            fin.add(new Object[]{d.getInteger("id"), d.getString("name"), d.getString("department"),
                d.getInteger("marks"), d.getString("grade") != null ? d.getString("grade") : "-",
                d.getString("city")});
        table(new String[]{"ID","Name","Dept","Mark","Grd","City"}, fin, new int[]{3,14,5,4,4,10});
        System.out.println("\n  Remaining documents: " + col.countDocuments());

        // ── Cleanup ─────────────────────────────────────────
        col.drop();
        client.close();

        banner("Lab 08 - Completed Successfully");
        System.out.println("  All CRUD + Analysis operations executed cleanly.\n");
    }
}
