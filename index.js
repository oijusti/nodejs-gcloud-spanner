const { Spanner } = require("@google-cloud/spanner");
const { v4: uuidv4 } = require("uuid");

// Configuration constants
const SPANNER_EMULATOR_HOST = "localhost:9010";
const PROJECT_ID = "test-project";
const INSTANCE_NAME = "test-instance";
const DATABASE_NAME = "test-db";
const CONFIG_NAME = "emulator";
const DISPLAY_NAME = "Test Instance";

// Set the Spanner emulator host
process.env.SPANNER_EMULATOR_HOST = SPANNER_EMULATOR_HOST;

const spanner = new Spanner({
  projectId: PROJECT_ID,
});

const instance = spanner.instance(INSTANCE_NAME);
const database = instance.database(DATABASE_NAME);

async function setupSpanner() {
  console.log("🚀 Creating Spanner Instance...");
  try {
    await instance.create({
      config: CONFIG_NAME,
      nodes: 1,
      displayName: DISPLAY_NAME,
    });
    console.log("✅ Spanner Instance Created!");
  } catch (err) {
    if (err.code === 6) {
      console.log("⚠️ Spanner Instance already exists. Skipping creation.");
    } else {
      throw err;
    }
  }

  console.log("🛠 Creating Spanner Database...");
  try {
    await database.create();
    console.log("✅ Spanner Database Created!");
  } catch (err) {
    if (err.code === 6) {
      console.log("⚠️ Spanner Database already exists. Skipping creation.");
    } else {
      throw err;
    }
  }

  console.log("🗄 Creating User Table...");
  const createTableQuery = `
    CREATE TABLE User (
      id STRING(36) NOT NULL,
      firstName STRING(100),
      lastName STRING(100),
      age INT64,
    ) PRIMARY KEY(id)
  `;
  try {
    await database.updateSchema([createTableQuery]);
    console.log("✅ User Table Created!");
  } catch (err) {
    if (err.code === 9) {
      console.log("⚠️ User Table already exists. Skipping creation.");
    } else {
      throw err;
    }
  }

  console.log("✅ Spanner Instance & Database Ready!");
}

async function checkConnection() {
  const query = {
    sql: "SELECT CURRENT_TIMESTAMP()",
  };

  try {
    const [rows] = await database.run(query);
    console.log("Connection successful. Current timestamp:", rows[0][0].value);
  } catch (err) {
    console.error("Error checking connection:", err);
  }
}

async function insertUser() {
  const query = {
    sql: `INSERT INTO User (id, firstName, lastName, age)
          VALUES (@id, @firstName, @lastName, @age)`,
    params: {
      id: uuidv4(),
      firstName: `Jane`,
      lastName: `Doe`,
      age: 36,
    },
  };

  try {
    await database.runTransactionAsync(async (transaction) => {
      const [rowCount] = await transaction.runUpdate(query);
      console.log(`Inserted ${rowCount} record(s).`);
      await transaction.commit();
    });
  } catch (err) {
    console.error("Error inserting record:", err);
  }
}

async function getAllUsers() {
  const query = {
    sql: "SELECT * FROM User",
  };

  try {
    const [rows] = await database.run(query);
    const users = rows.map((row) => {
      const user = {};
      row.forEach((field) => {
        user[field.name] = field.value;
      });
      return user;
    });
    console.log("All users:", JSON.stringify(users));
    return users;
  } catch (err) {
    console.error("Error getting all users:", err);
    return [];
  }
}

async function main() {
  await setupSpanner();
  await checkConnection();
  await insertUser();
  await getAllUsers();
}

main().catch(console.error);
