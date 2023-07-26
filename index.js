const geolib = require("geolib");
const PriorityQueue = require("priorityqueuejs");
const express = require("express");
const fs = require("firebase-admin");
const Redis = require("ioredis");
const PORT = 5000;

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
let flag = false;

const serviceAccount = require("./key.json");
fs.initializeApp({
  credential: fs.credential.cert(serviceAccount),
});
const db = fs.firestore();

const waitingCustomers = []; // Queue for waiting customers
let url =
  "redis://default:yhngclWW3rkEWaBXa159t4t1n0NvfoBA@redis-15456.c263.us-east-1-2.ec2.cloud.redislabs.com:15456";
const redisClient = new Redis(url);

async function saveDriversToRedis() {
  try {
    console.log("Connecting to Redis server...");

    //console.log("Fetching drivers from Firestore...");
    const driversSnapshot = await db
      .collection("drivers")
      .where("online", "==", true)
      .get();

    driversSnapshot.forEach((driverDoc) => {
      const driverData = driverDoc.data();
      const driverId = driverDoc.id;
      const driverString = JSON.stringify(driverData);
      //   console.log("dd ", driverData);

      // Save driver data to Redis
      redisClient
        .hset("driverId", driverId, driverString)
        .then(() => {
          //console.log(`Driver data saved to Redis: ${driverId}`);
        })
        .catch((err) => {
          console.error("Error saving driver data to Redis:", err);
        });
    });
  } catch (error) {
    console.error("Error retrieving drivers from Firestore:", error);
    throw error;
  }
}

async function getDriverFromRedis(driverId) {
  try {
    const driverData = await redisClient.hget("driverId", driverId);
    // console.log("-------------------------------------------",driverData);
    return JSON.parse(driverData);
  } catch (err) {
    throw err;
  }
}

async function nearestDriver(customerLocation) {
  await saveDriversToRedis();
  try {
    const driversSnapshot = await redisClient.hgetall("driverId");

    const driverQueue = new PriorityQueue((a, b) => a.distance < b.distance);

    Object.entries(driversSnapshot).forEach(async ([id, driver]) => {
      const driverId = id;

      const driverData = JSON.parse(driver);
      const { _latitude, _longitude } = driverData.location;
      const driverLocation = driverData.location;
      // console.log("loc", driverLocation._latitude);
      const distance = geolib.getDistance(
        [
          customerLocation.location.latitude,
          customerLocation.location.longitude,
        ],
        [driverLocation._latitude, driverLocation._longitude]
      );

      if (distance <= 500 && driverData.driverInQueue) {
        const driverName = driverData.Name;
        //console.log("DName",driverName);
        driverQueue.enq({
          driverId: driverId,
          location: driverLocation,
          distance: distance,
          name: driverName,
        });
      }
    });

    if (driverQueue.isEmpty()) {
      console.log("No driver found, pushing customer to the end");
      waitingCustomers.push(customerLocation);
      flag = false;
      return;
    }

    let driverDequeued = false;
    while (!driverQueue.isEmpty()) {
      const nearestDrivers = driverQueue.deq();
      const { driverId, distance } = nearestDrivers;

      const driverData = await getDriverFromRedis(driverId);
      if (driverData.driverInQueue) {
        const existingValue = await redisClient.hget("driverId", driverId);
        if (existingValue) {
          const parsedValue = JSON.parse(existingValue);
          parsedValue.driverInQueue = false;
          const driverRef = db.collection("drivers").doc(driverId);
          await driverRef.update({ driverInQueue: false });
          parsedValue.driverInQueue = false;
          const updatedValue = JSON.stringify(parsedValue);
          await redisClient.hset("driverId", driverId, updatedValue);
        } else {
          console.error(`error`);
        }

        redisClient
          .set(driverId, JSON.stringify(driverData))
          .then(() => {
            //console.log(`Driver data updated in Redis: ${driverId}`);
          })
          .catch((err) => {
            console.error("Error updating driver data in Redis:", err);
          });

        driverDequeued = true;
        flag = false;
        console.log("     DRIVER ALLOCATED   --->>");
        console.log("driverid", driverId);
        console.log("Distance: ", distance);
        // console.log("Driver dequeued:", driverData);

        async function updateDriverInQueueStatus(driverId) {
          try {
            const existingValue = await redisClient.hget("driverId", driverId);
            if (existingValue) {
              const parsedValue = JSON.parse(existingValue);
              parsedValue.driverInQueue = true;
              const updatedValue = JSON.stringify(parsedValue);
              await redisClient.hset("driverId", driverId, updatedValue);
              // console.log("DriverInQueue status updated");
            } else {
              console.error("Driver not found in Redis");
            }
          } catch (error) {
            console.error("Error updating DriverInQueue status:", error);
            throw error;
          }
        }

        (async () => {
          const capturedDriverId = driverId;
          setTimeout(async () => {
            const driverRef = db.collection("drivers").doc(capturedDriverId); // Use the captured driverId
            await driverRef.update({ driverInQueue: true });
            updateDriverInQueueStatus(driverId);
            console.log("Driver available: ", driverId);
          }, 30000);
        })();
        break;
      } else {
        continue;
      }
    }

    if (!driverDequeued) {
      console.log("No available driver found, pushing customer to the end");
      waitingCustomers.push(customerLocation);
      flag = false;
    }
  } catch (error) {
    console.error("Error finding nearest driver:", error);
    throw error;
  }
}

app.post("/customerRequest", async (req, res) => {
  const { id } = req.body;
  const userData = (await db.collection("users").doc(id).get()).data();
  waitingCustomers.push(userData);
  res.send(id);
});

function processCustomerRequest() {
  if (waitingCustomers.length === 0) {
    return;
  }
  if (!flag) {
    const customer = waitingCustomers.shift();
    nearestDriver(customer);
    flag = true;
  }
}

setInterval(() => {
  processCustomerRequest();
}, 5000);

app.listen(PORT, () => {
  console.log(`Server is running on ${PORT}`);
});
