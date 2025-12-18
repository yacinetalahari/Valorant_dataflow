db = db.getSiblingDB("valorant_stats");

// Create user for the application
db.createUser({
  user: "valorant_user",
  pwd: "valorant123",
  roles: [{ role: "readWrite", db: "valorant_stats" }],
});

// Create collections
db.createCollection("player_stats");
db.createCollection("realtime_stats");
db.createCollection("match_history");

// Create indexes
db.player_stats.createIndex({ player: 1 }, { unique: true });
db.player_stats.createIndex({ tier: 1 });
db.player_stats.createIndex({ kills: -1 });
db.realtime_stats.createIndex({ player: 1 });
db.realtime_stats.createIndex({ timestamp: -1 });

print("✅ MongoDB initialized with valorant_stats database!");
