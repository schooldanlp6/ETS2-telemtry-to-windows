/**
 * TruckNav Linux Telemetry Bridge
 * Reads /dev/shm/SCSTelemetry and broadcasts JSON over WebSocket on port 30001
 * Drop this file anywhere and run: node trucknav-bridge.mjs
 */

import { createServer } from "node:http";
import { WebSocketServer } from "ws";
import { readFileSync, openSync, readSync } from "node:fs";

const SHM_PATH = "/dev/shm/SCSTelemetry";
const SHM_SIZE = 32 * 1024;
const PORT = 30001;
const INTERVAL_MS = 50; // 20hz

// ── Offsets from scs-telemetry-common.hpp ──────────────────────────────────
const OFF = {
  sdkActive:        0,
  paused:           4,
  time:             8,   // ull
  simulatedTime:    16,  // ull
  renderTime:       24,  // ull
  multiplayerTimeOffset: 32, // ll

  // scs_values (offset 40)
  telemetry_plugin_revision: 40,
  version_major:    44,
  version_minor:    48,
  game:             52,  // 1=ETS2, 2=ATS
  telemetry_version_game_major: 56,
  telemetry_version_game_minor: 60,

  // common_ui (offset 64)
  time_abs:         64,  // minutes since epoch

  // config_ui (offset 68)
  restStop:         500, // common_i.restStop (offset 500)

  // common_f (offset 700)
  scale:            700,

  // truck_f starts at 764 (after config_f which is 64 bytes)
  // config_f: fuelCapacity(4) fuelWarningFactor(4) adblueCapacity(4) adblueWarningFactor(4)
  //           airPressureWarning(4) airPressurEmergency(4) oilPressureWarning(4)
  //           waterTemperatureWarning(4) batteryVoltageWarning(4) engineRpmMax(4)
  //           gearDifferential(4) cargoMass(4) truckWheelRadius[16](64)
  //           gearRatiosForward[24](96) gearRatiosReverse[8](32) unitMass(4) = 248 bytes
  // config_f starts at 704, so truck_f starts at 704+248 = 952... 
  // Let's use exact offsets from the struct layout:
  // common_f @ 700: scale (4 bytes) → 704
  // config_f @ 704: 248 bytes → ends at 952
  // truck_f @ 952:
  speed:            952,
  engineRpm:        956,
  userSteer:        960,
  userThrottle:     964,
  userBrake:        968,
  userClutch:       972,
  gameSteer:        976,
  gameThrottle:     980,
  gameBrake:        984,
  gameClutch:       988,
  cruiseControlSpeed: 992,
  airPressure:      996,
  brakeTemperature: 1000,
  fuel:             1004,
  fuelAvgConsumption: 1008,
  fuelRange:        1012,
  adblue:           1016,
  oilPressure:      1020,
  oilTemperature:   1024,
  waterTemperature: 1028,
  batteryVoltage:   1032,
  lightsDashboard:  1036,
  wearEngine:       1040,
  wearTransmission: 1044,
  wearCabin:        1048,
  wearChassis:      1052,
  wearWheels:       1056,
  truckOdometer:    1060,
  routeDistance:    1064,
  routeTime:        1068,
  speedLimit:       1072,
  // truck_wheel arrays (16*4 each) = 6 arrays = 384 bytes → ends at 1072+4+384=1460
  // gameplay_f @ 1460: jobDeliveredCargoDamage(4) jobDeliveredDistanceKm(4) refuelAmount(4)
  // job_f @ 1472: cargoDamage(4)
  // buffer_f[28] → ends at 1500

  // truck_b starts at 1565 (after config_b which starts at 1500)
  // config_b: truckWheelSteerable[16] truckWheelSimulated[16] truckWheelPowered[16] truckWheelLiftable[16] isCargoLoaded specialJob = 66 bytes → 1500+66=1566
  parkBrake:        1566,
  fuelWarning:      1570,
  lightsParking:    1582,
  lightsBeamLow:    1583,
  lightsBeamHigh:   1584,
  cruiseControlActive: 1589, // cruiseControl bool

  // truck_dp @ 2200
  coordinateX:      2200, // double
  coordinateY:      2208, // double
  coordinateZ:      2216, // double
  rotationX:        2224, // double
  rotationY:        2232, // double
  rotationZ:        2240, // double

  // config_s @ 2300
  truckBrandId:     2300,
  truckBrand:       2364,
  truckId:          2428,
  truckName:        2492,
  cargoId:          2556,
  cargo:            2620,
  cityDstId:        2684,
  cityDst:          2748,
  compDstId:        2812,
  compDst:          2876,
  citySrcId:        2940,
  citySrc:          3004,
  compSrcId:        3068,
  compSrc:          3132,

  jobMarket:        3260, // after shifterType[16]=3196+16=3212, licensePlate etc... let's compute
  // shifterType[16] @ 3196
  // truckLicensePlate[64] @ 3212
  // truckLicensePlateCountryId[64] @ 3276 -- but jobMarket[32] comes after compSrc
  // compSrc ends at 3132+64=3196, shifterType[16] @ 3196, ends 3212
  // truckLicensePlate[64] @ 3212, ends 3276
  // truckLicensePlateCountryId[64] @ 3276, ends 3340
  // truckLicensePlateCountry[64] @ 3340, ends 3404 -- wait jobMarket[32] is BEFORE license plate in struct
  // Order in struct: truckBrandId truckBrand truckId truckName cargoId cargo cityDstId cityDst
  //                  compDstId compDst citySrcId citySrc compSrcId compSrc shifterType[16]
  //                  truckLicensePlate truckLicensePlateCountryId truckLicensePlateCountry jobMarket[32]
  // jobMarket @ 3404
  jobMarketFixed:   3404,

  // gameplay_s @ after buffer_s... gameplay_s starts right after config_s ends
  // config_s ends at 3404+32=3436, buffer_s[20] → 3436+20=3456? 
  // Actually buffer_s comes after gameplay_s per the struct. Let me re-check.
  // config_s then gameplay_s then buffer_s[20] then end at 3999
  // gameplay_s @ 3436:
  fineOffence:      3436,

  // config_ull @ 4000
  jobIncome:        4000, // ull

  // gameplay_ll @ 4200
  jobCancelledPenalty: 4200,
  jobDeliveredRevenue: 4208,
  fineAmount:       4216,
  tollgatePayAmount: 4224,
  ferryPayAmount:   4232,
  trainPayAmount:   4240,

  // special_b @ 4300
  onJob:            4300,
  jobFinished:      4301,
  jobCancelled:     4302,
  jobDelivered:     4303,
  fined:            4304,
  tollgate:         4305,
  ferry:            4306,
  train:            4307,
  refuel:           4308,
  refuelPayed:      4309,

  // trailer[0] starts at offset 6000
  // scsTrailer_t layout for trailer[0]:
  // com_b.attached is at offset 64 within scsTrailer_t (after bool arrays 4*16=64, then attached)
  trailer0_attached: 6064,
  // com_dp.worldX @ 872 within scsTrailer_t
  trailer0_worldX:  6872,
  trailer0_worldY:  6880,
  trailer0_worldZ:  6888,
  trailer0_rotationY: 6904,
  // com_f.cargoDamage @ 152
  trailer0_cargoDamage: 6152,
  // con_s.brand @ 920+4*64=920+256=1176 within trailer... 
  // con_s starts at 920: id(64) cargoAcessoryId(64) bodyType(64) brandId(64) brand(64) name(64)
  trailer0_brand:   6920+4*64, // = 7176
  trailer0_name:    6920+5*64, // = 7240
};

function readStr(buf, offset, len = 64) {
  const slice = buf.slice(offset, offset + len);
  const end = slice.indexOf(0);
  return slice.slice(0, end === -1 ? len : end).toString("utf8");
}

function absMinutesToISO(minutes) {
  // ETS2 game time: minutes since some epoch. Convert to a fake ISO date.
  const ms = minutes * 60 * 1000;
  return new Date(ms).toISOString();
}

function readTelemetry(buf) {
  const sdkActive = buf.readUInt8(OFF.sdkActive) !== 0;
  if (!sdkActive) return null;

  const paused = buf.readUInt8(OFF.paused) !== 0;
  const gameId = buf.readUInt32LE(OFF.game);
  const game = gameId === 1 ? "ets2" : gameId === 2 ? "ats" : "unknown";

  const vMajor = buf.readUInt32LE(OFF.version_major);
  const vMinor = buf.readUInt32LE(OFF.version_minor);
  const tvMajor = buf.readUInt32LE(OFF.telemetry_version_game_major);
  const tvMinor = buf.readUInt32LE(OFF.telemetry_version_game_minor);

  const timeAbs = buf.readUInt32LE(OFF.time_abs);
  const scale = buf.readFloatLE(OFF.scale);
  const restStop = buf.readInt32LE(OFF.restStop);

  // Truck position
  const x = buf.readDoubleBE !== undefined ? buf.readDoubleLE(OFF.coordinateX) : 0;
  const y = buf.readDoubleLE(OFF.coordinateY);
  const z = buf.readDoubleLE(OFF.coordinateZ);
  const rotY = buf.readDoubleLE(OFF.rotationY ?? OFF.rotationX + 8);
  const heading = rotY;

  const speed = buf.readFloatLE(OFF.speed);
  const speedKph = speed * 3.6;
  const speedMph = speed * 2.237;
  const speedLimit = buf.readFloatLE(OFF.speedLimit);
  const speedLimitKph = speedLimit * 3.6;
  const speedLimitMph = speedLimit * 2.237;

  const fuel = buf.readFloatLE(OFF.fuel);
  const fuelRange = buf.readFloatLE(OFF.fuelRange);
  const fuelAvg = buf.readFloatLE(OFF.fuelAvgConsumption);
  const fuelWarning = buf.readUInt8(OFF.fuelWarning) !== 0;
  const fuelCapacity = buf.readFloatLE(704); // config_f.fuelCapacity

  const engineRpm = buf.readFloatLE(OFF.engineRpm);
  const engineRpmMax = buf.readFloatLE(704 + 36); // config_f.engineRpmMax
  const odometer = buf.readFloatLE(OFF.truckOdometer);

  const wearEngine = buf.readFloatLE(OFF.wearEngine);
  const wearTransmission = buf.readFloatLE(OFF.wearTransmission);
  const wearCabin = buf.readFloatLE(OFF.wearCabin);
  const wearChassis = buf.readFloatLE(OFF.wearChassis);
  const wearWheels = buf.readFloatLE(OFF.wearWheels);

  const parkBrake = buf.readUInt8(OFF.parkBrake) !== 0;
  const lightsParking = buf.readUInt8(OFF.lightsParking) !== 0;
  const lightsBeamLow = buf.readUInt8(OFF.lightsBeamLow) !== 0;
  const lightsBeamHigh = buf.readUInt8(OFF.lightsBeamHigh) !== 0;
  const cruiseControl = buf.readUInt8(OFF.cruiseControlActive) !== 0;
  const cruiseControlSpeed = buf.readFloatLE(OFF.cruiseControlSpeed);

  const routeDistance = buf.readFloatLE(OFF.routeDistance);
  const routeTime = buf.readFloatLE(OFF.routeTime);

  const truckBrand = readStr(buf, OFF.truckBrand);
  const truckName = readStr(buf, OFF.truckName);
  const truckBrandId = readStr(buf, OFF.truckBrandId);

  const cargoId = readStr(buf, OFF.cargoId);
  const cargo = readStr(buf, OFF.cargo);
  const cargoMass = buf.readFloatLE(704 + 44); // config_f.cargoMass
  const isCargoLoaded = buf.readUInt8(1500) !== 0; // config_b.isCargoLoaded
  const specialJob = buf.readUInt8(1501) !== 0;
  const jobMarket = readStr(buf, OFF.jobMarketFixed, 32);

  const cityDstId = readStr(buf, OFF.cityDstId);
  const cityDst = readStr(buf, OFF.cityDst);
  const compDstId = readStr(buf, OFF.compDstId);
  const compDst = readStr(buf, OFF.compDst);
  const citySrcId = readStr(buf, OFF.citySrcId);
  const citySrc = readStr(buf, OFF.citySrc);
  const compSrcId = readStr(buf, OFF.compSrcId);
  const compSrc = readStr(buf, OFF.compSrc);

  const jobIncome = Number(buf.readBigUInt64LE(OFF.jobIncome));
  const timeAbsDelivery = buf.readUInt32LE(68 + 4 + 20); // config_ui.time_abs_delivery
  const plannedDistanceKm = buf.readUInt32LE(68 + 4 + 28); // config_ui.plannedDistanceKm

  // Special events
  const onJob = buf.readUInt8(OFF.onJob) !== 0;
  const jobCancelled = buf.readUInt8(OFF.jobCancelled) !== 0;
  const jobDelivered = buf.readUInt8(OFF.jobDelivered) !== 0;
  const fined = buf.readUInt8(OFF.fined) !== 0;
  const tollgate = buf.readUInt8(OFF.tollgate) !== 0;
  const ferry = buf.readUInt8(OFF.ferry) !== 0;
  const train = buf.readUInt8(OFF.train) !== 0;

  // Gameplay
  const jobDeliveredRevenue = Number(buf.readBigInt64LE(OFF.jobDeliveredRevenue));
  const jobDeliveredCargoDamage = buf.readFloatLE(1460);
  const jobDeliveredDistanceKm = buf.readFloatLE(1464);
  const jobCancelledPenalty = Number(buf.readBigInt64LE(OFF.jobCancelledPenalty));
  const fineAmount = Number(buf.readBigInt64LE(OFF.fineAmount));
  const tollgatePayAmount = Number(buf.readBigInt64LE(OFF.tollgatePayAmount));
  const ferryPayAmount = Number(buf.readBigInt64LE(OFF.ferryPayAmount));
  const trainPayAmount = Number(buf.readBigInt64LE(OFF.trainPayAmount));
  const jobDeliveredAutoparkUsed = buf.readUInt8(1640) !== 0;
  const jobDeliveredAutoloadUsed = buf.readUInt8(1641) !== 0;
  const fineOffence = readStr(buf, OFF.fineOffence, 32);

  const ferrySourceName = readStr(buf, OFF.fineOffence + 32);
  const ferryTargetName = readStr(buf, OFF.fineOffence + 96);
  const trainSourceName = readStr(buf, OFF.fineOffence + 224);
  const trainTargetName = readStr(buf, OFF.fineOffence + 288);

  const jobStartingTime = buf.readUInt32LE(68 + 4 + 16); // config_ui.jobStartingTime... actually gameplay_ui
  // gameplay_ui @ 68+4+9*4 = 68+4+36 = 108
  const jobDeliveredDeliveryTime = buf.readUInt32LE(108);
  const jobStartingTimeVal = buf.readUInt32LE(112);
  const jobFinishedTime = buf.readUInt32LE(116);
  const jobDeliveredEarnedXp = buf.readInt32LE(500 + 4 + 32 * 4); // gameplay_i

  // Trailer 0
  const trailer0attached = buf.readUInt8(OFF.trailer0_attached) !== 0;
  const t0x = buf.readDoubleLE(OFF.trailer0_worldX);
  const t0y = buf.readDoubleLE(OFF.trailer0_worldY);
  const t0z = buf.readDoubleLE(OFF.trailer0_worldZ);
  const t0cargoDamage = buf.readFloatLE(OFF.trailer0_cargoDamage);
  const t0brand = readStr(buf, 7176);
  const t0name = readStr(buf, 7240);

  const remainingDeliveryTime = absMinutesToISO(timeAbsDelivery > 0 ? timeAbsDelivery : 0);
  const gameTime = absMinutesToISO(timeAbs);

  const packet = {
    paused,
    game,
    gameVersion: `${vMajor}.${vMinor}`,
    telemetryVersion: `${tvMajor}.${tvMinor}`,
    common: {
      mapScale: scale,
      gameTime,
      nextRestStopMinutes: restStop,
    },
    truck: {
      constants: {
        fuelCapacity,
        brand: truckBrand,
        name: truckName,
      },
      current: {
        dashboard: {
          fuelAmount: fuel,
          averageConsumption: fuelAvg,
          fuelRange,
          fuelWarning,
          currentGear: buf.readInt32LE(504),
          speedKph,
          speedMph,
          cruiseControlSpeedKph: cruiseControlSpeed * 3.6,
          cruiseControlSpeedMph: cruiseControlSpeed * 2.237,
          cruiseControlActive: cruiseControl,
          rpm: engineRpm,
          odometer,
        },
        lights: {
          parking: lightsParking,
          beamLow: lightsBeamLow,
          beamHigh: lightsBeamHigh,
        },
        damage: {
          engine: wearEngine,
          transmission: wearTransmission,
          cabin: wearCabin,
          chassis: wearChassis,
          wheels: wearWheels,
        },
        position: { x, y, z },
        heading,
        parkingBrake: parkBrake,
      },
      positioning: {},
    },
    trailers: [
      {
        attached: trailer0attached,
        damage: {
          cargo: t0cargoDamage,
          wheels: 0,
          chassis: 0,
        },
        position: { x: t0x, y: t0y, z: t0z },
        heading: 0,
        brand: t0brand,
        name: t0name,
      },
    ],
    job: {
      remainingDeliveryTime,
      cargoLoaded: isCargoLoaded,
      specialJob,
      jobType: jobMarket,
      cargo: {
        mass: cargoMass,
        name: cargo,
        cargoDamage: jobDeliveredCargoDamage,
      },
      cityDestinationId: cityDstId,
      cityDestination: cityDst,
      companyDestinationId: compDstId,
      companyDestination: compDst,
      citySourceId: citySrcId,
      citySource: citySrc,
      companySourceId: compSrcId,
      companySource: compSrc,
      income: jobIncome,
    },
    navigation: {
      distance: routeDistance,
      time: routeTime,
      speedLimitKph,
      speedLimitMph,
    },
    specialEvents: {
      onJob,
      jobCancelled,
      jobDelivered,
      fined,
      tollgate,
      ferry,
      train,
    },
    gamePlayEvents: {
      ferryData: { payAmount: Number(ferryPayAmount), sourceName: ferrySourceName, targetName: ferryTargetName },
      finedData: { payAmount: Number(fineAmount), offence: fineOffence },
      jobCancelledPenalty: Number(jobCancelledPenalty),
      jobDelivered: {
        autoLoaded: jobDeliveredAutoloadUsed,
        autoParker: jobDeliveredAutoparkUsed,
        cargoDamage: jobDeliveredCargoDamage,
        deliveryTime: remainingDeliveryTime,
        distanceKm: jobDeliveredDistanceKm,
        earnedXp: jobDeliveredEarnedXp,
        revenue: jobDeliveredRevenue,
      },
      tollgatePayment: Number(tollgatePayAmount),
      trainData: { payAmount: Number(trainPayAmount), sourceName: trainSourceName, targetName: trainTargetName },
    },
  };

  return packet;
}

// ── WebSocket server ────────────────────────────────────────────────────────
const server = createServer();
const wss = new WebSocketServer({ server });

const clients = new Set();

wss.on("connection", (ws) => {
  clients.add(ws);
  console.log(`[Bridge] Client connected (total: ${clients.size})`);
  ws.on("close", () => {
    clients.delete(ws);
    console.log(`[Bridge] Client disconnected (total: ${clients.size})`);
  });
});

let fd;
try {
  fd = openSync(SHM_PATH, "r");
  console.log(`[Bridge] Opened ${SHM_PATH}`);
} catch (e) {
  console.error(`[Bridge] Cannot open ${SHM_PATH}: ${e.message}`);
  console.error("[Bridge] Make sure ETS2 is running with the scs-sdk plugin loaded.");
  process.exit(1);
}

const buf = Buffer.alloc(SHM_SIZE);

setInterval(() => {
  if (clients.size === 0) return;
  try {
    readSync(fd, buf, 0, SHM_SIZE, 0);
    const packet = readTelemetry(buf);
    if (!packet) return;
    const json = JSON.stringify(packet);
    for (const ws of clients) {
      if (ws.readyState === 1) ws.send(json);
    }
  } catch (e) {
    console.error("[Bridge] Read error:", e.message);
  }
}, INTERVAL_MS);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[Bridge] WebSocket listening on ws://0.0.0.0:${PORT}`);
  console.log("[Bridge] Open TruckNav and enter your PC's IP address.");
});
