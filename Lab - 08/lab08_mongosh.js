// ============================================================
//  Lab 08 – MongoDB CRUD & Data Analysis
//  Run with: mongosh lab08_mongosh.js
// ============================================================

function header(title) {
    const line = "=".repeat(60);
    print("\n" + line);
    print("  " + title);
    print(line);
}

function subheader(title) {
    print("\n--- " + title + " ---");
}

// ============================================================
//  SETUP: Drop previous data if it exists
// ============================================================
header("Lab 08: MongoDB – CRUD & Data Analysis");
print("  Database  : student_db");
print("  Collection: students");
print("  Date      : " + new Date().toDateString());

db = db.getSiblingDB("student_db");
db.students.drop();
print("\n[Setup] Previous collection dropped (if existed).");

// ============================================================
//  STEP 1: INSERT DOCUMENTS
// ============================================================
header("STEP 1 – INSERT DOCUMENTS");

subheader("1.1  Insert single document (insertOne)");
let result1 = db.students.insertOne({
    id: 1, name: "Amit Kumar",  department: "CSE",
    marks: 85, city: "Delhi",    year: 2
});
print("insertOne result:");
printjson(result1);

subheader("1.2  Insert 9 more documents (insertMany)");
let result2 = db.students.insertMany([
    { id: 2,  name: "Neha Sharma",  department: "IT",  marks: 90, city: "Mumbai",    year: 3 },
    { id: 3,  name: "Ravi Verma",   department: "CSE", marks: 72, city: "Chennai",   year: 1 },
    { id: 4,  name: "Priya Singh",  department: "ECE", marks: 88, city: "Pune",      year: 2 },
    { id: 5,  name: "Arjun Patel",  department: "CSE", marks: 94, city: "Ahmedabad", year: 3 },
    { id: 6,  name: "Sneha Gupta",  department: "IT",  marks: 76, city: "Delhi",     year: 2 },
    { id: 7,  name: "Karan Mehta",  department: "ECE", marks: 63, city: "Mumbai",    year: 1 },
    { id: 8,  name: "Divya Joshi",  department: "CSE", marks: 91, city: "Jaipur",    year: 3 },
    { id: 9,  name: "Rohit Das",    department: "IT",  marks: 55, city: "Kolkata",   year: 1 },
    { id: 10, name: "Meera Nair",   department: "ECE", marks: 82, city: "Kochi",     year: 2 }
]);
print("insertMany acknowledged: " + result2.acknowledged);
print("Total inserted:          " + Object.keys(result2.insertedIds).length);

// ============================================================
//  STEP 2: RETRIEVE DOCUMENTS
// ============================================================
header("STEP 2 – RETRIEVE DOCUMENTS");

subheader("2.1  All documents (name, department, marks, city)");
db.students.find({}, { _id: 0, id: 1, name: 1, department: 1, marks: 1, city: 1, year: 1 })
    .forEach(doc => print(
        "  [" + String(doc.id).padStart(2) + "]  " +
        doc.name.padEnd(14) + " | " +
        doc.department.padEnd(4) + " | " +
        "Marks: " + String(doc.marks).padStart(3) + " | " +
        "City: " + doc.city.padEnd(10) + " | " +
        "Year: " + doc.year
    ));

print("\nTotal documents: " + db.students.countDocuments());

subheader("2.2  Find specific student (Arjun Patel)");
printjson(db.students.findOne({ name: "Arjun Patel" }, { _id: 0 }));

subheader("2.3  All CSE students");
db.students.find({ department: "CSE" }, { _id: 0, name: 1, marks: 1 })
    .forEach(doc => print("  " + doc.name.padEnd(14) + " : " + doc.marks));

// ============================================================
//  STEP 3: FILTER QUERIES
// ============================================================
header("STEP 3 – FILTERED QUERIES");

subheader("3.1  Students with marks > 80");
db.students.find({ marks: { $gt: 80 } }, { _id: 0, name: 1, department: 1, marks: 1 })
    .forEach(doc => print("  " + doc.name.padEnd(14) + " | " + doc.department + " | Marks: " + doc.marks));

subheader("3.2  Students with marks < 70 (at risk)");
db.students.find({ marks: { $lt: 70 } }, { _id: 0, name: 1, department: 1, marks: 1 })
    .forEach(doc => print("  " + doc.name.padEnd(14) + " | " + doc.department + " | Marks: " + doc.marks));

subheader("3.3  CSE students with marks > 80 (AND filter)");
db.students.find({ department: "CSE", marks: { $gt: 80 } }, { _id: 0, name: 1, marks: 1 })
    .forEach(doc => print("  " + doc.name.padEnd(14) + " : " + doc.marks));

subheader("3.4  Students in CSE or ECE (OR filter using $or)");
db.students.find(
    { $or: [{ department: "CSE" }, { department: "ECE" }] },
    { _id: 0, name: 1, department: 1, marks: 1 }
).forEach(doc => print("  " + doc.name.padEnd(14) + " | " + doc.department + " | Marks: " + doc.marks));

subheader("3.5  Students in CSE or IT (using $in)");
db.students.find(
    { department: { $in: ["CSE", "IT"] } },
    { _id: 0, name: 1, department: 1 }
).forEach(doc => print("  " + doc.name.padEnd(14) + " | " + doc.department));

// ============================================================
//  STEP 4: SORTING & LIMITING
// ============================================================
header("STEP 4 – SORTING & RANK LIST");

subheader("4.1  All students sorted by marks (descending)");
let rank = 1;
db.students.find({}, { _id: 0, name: 1, department: 1, marks: 1 })
    .sort({ marks: -1 })
    .forEach(doc => {
        print("  Rank " + String(rank).padStart(2) + ": " +
              doc.name.padEnd(14) + " | " + doc.department + " | " + doc.marks);
        rank++;
    });

subheader("4.2  Top 3 performers");
db.students.find({}, { _id: 0, name: 1, marks: 1 })
    .sort({ marks: -1 })
    .limit(3)
    .forEach(doc => print("  " + doc.name.padEnd(14) + " : " + doc.marks));

// ============================================================
//  STEP 5: AGGREGATION PIPELINE
// ============================================================
header("STEP 5 – DATA ANALYSIS (AGGREGATION PIPELINE)");

subheader("5.1  Overall statistics");
let overall = db.students.aggregate([
    { $group: {
        _id: null,
        total_students: { $sum: 1 },
        average_marks:  { $avg: "$marks" },
        highest_marks:  { $max: "$marks" },
        lowest_marks:   { $min: "$marks" },
        total_marks:    { $sum: "$marks" }
    }}
]).toArray()[0];
print("  Total Students : " + overall.total_students);
print("  Average Marks  : " + overall.average_marks.toFixed(2));
print("  Highest Marks  : " + overall.highest_marks);
print("  Lowest Marks   : " + overall.lowest_marks);
print("  Total Marks    : " + overall.total_marks);

subheader("5.2  Department-wise statistics");
print("  " + "DEPT".padEnd(5) + " | " + "COUNT".padEnd(5) + " | " +
      "AVG".padEnd(7) + " | " + "MAX".padEnd(4) + " | MIN");
print("  " + "-".repeat(37));
db.students.aggregate([
    { $group: {
        _id: "$department",
        count:     { $sum: 1 },
        avg_marks: { $avg: "$marks" },
        max_marks: { $max: "$marks" },
        min_marks: { $min: "$marks" }
    }},
    { $sort: { avg_marks: -1 } }
]).forEach(d => print(
    "  " + d._id.padEnd(5) + " | " +
    String(d.count).padEnd(5) + " | " +
    d.avg_marks.toFixed(2).padEnd(7) + " | " +
    String(d.max_marks).padEnd(4) + " | " + d.min_marks
));

subheader("5.3  Departments with average marks > 75");
db.students.aggregate([
    { $group: { _id: "$department", avg_marks: { $avg: "$marks" } } },
    { $match: { avg_marks: { $gt: 75 } } },
    { $sort: { avg_marks: -1 } }
]).forEach(d => print("  " + d._id + " → avg: " + d.avg_marks.toFixed(2)));

subheader("5.4  Students per year of study");
db.students.aggregate([
    { $group: { _id: "$year", count: { $sum: 1 }, names: { $push: "$name" } } },
    { $sort: { _id: 1 } }
]).forEach(d => print("  Year " + d._id + " (" + d.count + " students): " + d.names.join(", ")));

subheader("5.5  Grade distribution ($bucket)");
print("  Grade | Range    | Count | Students");
print("  " + "-".repeat(55));
db.students.aggregate([
    { $bucket: {
        groupBy: "$marks",
        boundaries: [0, 60, 75, 90, 101],
        default: "Other",
        output: { count: { $sum: 1 }, students: { $push: "$name" } }
    }}
]).forEach(b => {
    let grade, range;
    if (b._id === 0)   { grade = "D"; range = "0–59  "; }
    else if (b._id === 60)  { grade = "C"; range = "60–74 "; }
    else if (b._id === 75)  { grade = "B"; range = "75–89 "; }
    else               { grade = "A"; range = "90–100"; }
    print("  " + grade + "     | " + range + " | " + String(b.count).padEnd(5) + " | " + b.students.join(", "));
});

subheader("5.6  City-wise student distribution");
db.students.aggregate([
    { $group: { _id: "$city", count: { $sum: 1 } } },
    { $sort: { count: -1, _id: 1 } }
]).forEach(c => print("  " + c._id.padEnd(12) + " : " + c.count));

// ============================================================
//  STEP 6: UPDATE DOCUMENTS
// ============================================================
header("STEP 6 – UPDATE DOCUMENTS");

subheader("6.1  Before update – Ravi Verma's marks");
let before = db.students.findOne({ id: 3 }, { _id: 0, name: 1, marks: 1 });
print("  Before: " + before.name + " → Marks: " + before.marks);

let upd1 = db.students.updateOne({ id: 3 }, { $set: { marks: 78 } });
print("  updateOne → matchedCount: " + upd1.matchedCount + ", modifiedCount: " + upd1.modifiedCount);

let after = db.students.findOne({ id: 3 }, { _id: 0, name: 1, marks: 1 });
print("  After : " + after.name + " → Marks: " + after.marks + "  ← Updated (+6)");

subheader("6.2  Give 5 bonus marks to all IT students");
print("  IT students BEFORE bonus:");
db.students.find({ department: "IT" }, { _id: 0, name: 1, marks: 1 })
    .forEach(d => print("    " + d.name.padEnd(14) + " : " + d.marks));

let upd2 = db.students.updateMany({ department: "IT" }, { $inc: { marks: 5 } });
print("  updateMany → matchedCount: " + upd2.matchedCount + ", modifiedCount: " + upd2.modifiedCount);

print("  IT students AFTER bonus (+5):");
db.students.find({ department: "IT" }, { _id: 0, name: 1, marks: 1 })
    .forEach(d => print("    " + d.name.padEnd(14) + " : " + d.marks));

subheader("6.3  Assign grades to all students");
db.students.updateMany({ marks: { $gte: 90 } },           { $set: { grade: "A" } });
db.students.updateMany({ marks: { $gte: 75, $lt: 90 } },  { $set: { grade: "B" } });
db.students.updateMany({ marks: { $gte: 60, $lt: 75 } },  { $set: { grade: "C" } });
db.students.updateMany({ marks: { $lt: 60 } },            { $set: { grade: "D" } });
print("  Grades assigned successfully.");
db.students.find({}, { _id: 0, name: 1, marks: 1, grade: 1 })
    .sort({ marks: -1 })
    .forEach(d => print("  " + d.name.padEnd(14) + " | Marks: " + String(d.marks).padStart(3) + " | Grade: " + d.grade));

// ============================================================
//  STEP 7: DELETE DOCUMENTS
// ============================================================
header("STEP 7 – DELETE DOCUMENTS");

print("  Total before delete: " + db.students.countDocuments());

subheader("7.1  Delete Rohit Das (id: 9) – deleteOne");
let del1 = db.students.deleteOne({ id: 9 });
print("  deleteOne → deletedCount: " + del1.deletedCount);
print("  Total after delete : " + db.students.countDocuments());
print("  Find Rohit Das: " + db.students.findOne({ id: 9 }));

subheader("7.2  Delete grade D students – deleteMany");
let del2 = db.students.deleteMany({ grade: "D" });
print("  deleteMany (grade D) → deletedCount: " + del2.deletedCount);
print("  Total after delete: " + db.students.countDocuments());

// ============================================================
//  STEP 8: INDEXING
// ============================================================
header("STEP 8 – INDEXING");

subheader("8.1  Create index on department field");
let idx1 = db.students.createIndex({ department: 1 });
print("  Index created: " + idx1);

subheader("8.2  Create compound index (department + marks)");
let idx2 = db.students.createIndex({ department: 1, marks: -1 });
print("  Index created: " + idx2);

subheader("8.3  List all indexes");
db.students.getIndexes().forEach(i => print("  Index: " + JSON.stringify(i.key) + "  →  name: " + i.name));

// ============================================================
//  FINAL SUMMARY
// ============================================================
header("FINAL STATE – All Remaining Documents");
db.students.find({}, { _id: 0, id: 1, name: 1, department: 1, marks: 1, grade: 1 })
    .sort({ marks: -1 })
    .forEach(doc => print(
        "  [" + String(doc.id).padStart(2) + "]  " +
        doc.name.padEnd(14) + " | " +
        doc.department.padEnd(4) + " | " +
        "Marks: " + String(doc.marks).padStart(3) + " | " +
        "Grade: " + doc.grade
    ));

print("\n  Remaining documents: " + db.students.countDocuments());

header("Lab 08 – Completed Successfully");
print("  All CRUD operations and analysis queries executed.");
print("  Collection: students | Database: student_db\n");
