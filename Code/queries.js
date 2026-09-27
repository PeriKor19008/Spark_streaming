// Ενδεικτικά queries επαλήθευσης πάνω στη βάση `traffic`.
// Εκτέλεση:  docker exec -i mongo mongosh --quiet < queries.js

const d = db.getSiblingDB("traffic");

// Σημείωση: στη mongosh το `db.stats` είναι built-in μέθοδος, οπότε η collection
// με το όνομα `stats` προσπελαύνεται υποχρεωτικά μέσω getCollection().
const stats = d.getCollection("stats");

print("=== Πλήθος documents ανά collection ===");
print("raw_data:     " + d.raw_data.countDocuments());
print("stats:        " + stats.countDocuments());
print("window_stats: " + d.window_stats.countDocuments());

print("\n=== Δείγμα raw_data ===");
printjson(d.raw_data.findOne({}, { _id: 0 }));

print("\n=== Δείγμα stats ===");
printjson(stats.find({}, { _id: 0 }).sort({ vcount: -1 }).limit(3).toArray());

print("\n=== Μέση ταχύτητα ανά ακμή σε όλο το διάστημα της εξομοίωσης ===");
printjson(
  stats.aggregate([
    {
      $group: {
        _id: "$link",
        avg_speed: { $avg: "$vspeed" },
        max_speed: { $max: "$vmax" },
        min_speed: { $min: "$vmin" },
        total_vehicles: { $sum: "$vcount" },
        samples: { $sum: 1 },
      },
    },
    {
      $project: {
        _id: 0,
        link: "$_id",
        avg_speed: { $round: ["$avg_speed", 2] },
        max_speed: 1,
        min_speed: 1,
        total_vehicles: 1,
        samples: 1,
      },
    },
    { $sort: { avg_speed: 1 } },
  ]).toArray()
);

print("\n=== Windowed aggregation (30s) — πρώτα παράθυρα ===");
printjson(
  d.window_stats.find({}, { _id: 0 })
    .sort({ window_start: 1, link: 1 })
    .limit(5)
    .toArray()
);
