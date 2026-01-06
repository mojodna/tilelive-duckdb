"use strict";

const { describe, it, before, after } = require("node:test");
const assert = require("node:assert");
const fs = require("node:fs");
const path = require("node:path");
const { DuckDBInstance } = require("@duckdb/node-api");

describe("tilelive-duckdb", () => {
  it("exports a function that takes tilelive", () => {
    const tilelive = { protocols: {} };
    const createSource = require("../index.js");

    assert.strictEqual(typeof createSource, "function");

    const DuckDBSource = createSource(tilelive);

    assert.strictEqual(typeof DuckDBSource, "function");
  });

  it("registers duckdb: protocol", () => {
    const tilelive = { protocols: {} };
    require("../index.js")(tilelive);

    assert.strictEqual(typeof tilelive.protocols["duckdb:"], "function");
  });
});

describe("DuckDBSource constructor", () => {
  const fixturesDir = path.join(__dirname, "fixtures");
  const paramsDbPath = path.join(fixturesDir, "params.db");

  before(async () => {
    fs.mkdirSync(fixturesDir, { recursive: true });

    if (fs.existsSync(paramsDbPath)) {
      fs.unlinkSync(paramsDbPath);
    }

    const instance = await DuckDBInstance.create(paramsDbPath);
    const conn = await instance.connect();

    await conn.run("INSTALL spatial; LOAD spatial;");
    await conn.run(`
      CREATE TABLE buildings (
        id INTEGER,
        geom GEOMETRY
      )
    `);
    await conn.run(`
      CREATE TABLE points (
        id INTEGER,
        geometry GEOMETRY
      )
    `);

    conn.closeSync();
  });

  after(() => {
    if (fs.existsSync(paramsDbPath)) {
      fs.unlinkSync(paramsDbPath);
    }
  });

  it("parses URI parameters", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(
      `duckdb://${paramsDbPath}?table=buildings&geometry=geom&layer=structures`
    );

    new DuckDBSource(uri, (err, source) => {
      assert.ifError(err);
      assert.strictEqual(source.dbPath, paramsDbPath);
      assert.strictEqual(source.table, "buildings");
      assert.strictEqual(source.geometryColumn, "geom");
      assert.strictEqual(source.layerName, "structures");
      source.close(done);
    });
  });

  it("uses default geometry column and layer name", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(`duckdb://${paramsDbPath}?table=points`);

    new DuckDBSource(uri, (err, source) => {
      assert.ifError(err);
      assert.strictEqual(source.geometryColumn, "geometry");
      assert.strictEqual(source.layerName, "points");
      source.close(done);
    });
  });

  it("errors when table parameter is missing", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(`duckdb://${paramsDbPath}`);

    new DuckDBSource(uri, (err) => {
      assert.ok(err);
      assert.match(err.message, /table.*required/i);
      done();
    });
  });

  it("handles relative database paths", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    // Create a relative URI - relative to CWD (project root)
    const relativeUri = `duckdb://./test/fixtures/params.db?table=buildings&geometry=geom`;

    new DuckDBSource(relativeUri, (err, source) => {
      assert.ifError(err);
      // Should resolve to absolute path
      assert.ok(
        path.isAbsolute(source.dbPath),
        `Expected absolute path, got: ${source.dbPath}`
      );
      // Should match the absolute path we expect
      assert.strictEqual(source.dbPath, paramsDbPath);
      source.close(done);
    });
  });

  it("rejects SQL injection in table name", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(
      `duckdb://${paramsDbPath}?table=buildings'; DROP TABLE buildings;--`
    );

    new DuckDBSource(uri, (err) => {
      assert.ok(err, "should return an error for invalid table name");
      assert.match(err.message, /invalid.*table/i, "should mention invalid table");
      done();
    });
  });

  it("rejects SQL injection in geometry column name", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(
      `duckdb://${paramsDbPath}?table=buildings&geometry=geom'; DROP TABLE buildings;--`
    );

    new DuckDBSource(uri, (err) => {
      assert.ok(err, "should return an error for invalid geometry column");
      assert.match(err.message, /invalid.*column/i, "should mention invalid column");
      done();
    });
  });
});

describe("DuckDBSource connection", () => {
  const fixturesDir = path.join(__dirname, "fixtures");
  const testDbPath = path.join(fixturesDir, "test.db");

  before(async () => {
    fs.mkdirSync(fixturesDir, { recursive: true });

    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }

    const instance = await DuckDBInstance.create(testDbPath);
    const conn = await instance.connect();

    await conn.run("INSTALL spatial; LOAD spatial;");
    await conn.run(`
      CREATE TABLE points (
        id INTEGER,
        name VARCHAR,
        geometry GEOMETRY
      )
    `);
    await conn.run(`
      INSERT INTO points VALUES
        (1, 'Point A', ST_GeomFromText('POINT(0 0)')),
        (2, 'Point B', ST_GeomFromText('POINT(1000000 1000000)'))
    `);

    conn.closeSync();
  });

  after(() => {
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
  });

  it("opens database connection", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(`duckdb://${testDbPath}?table=points`);

    new DuckDBSource(uri, (err, source) => {
      assert.ifError(err);
      assert.ok(source.connection, "should have connection property");
      source.close(done);
    });
  });

  it("errors on non-existent database", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL("duckdb:///nonexistent/path/db.db?table=foo");

    new DuckDBSource(uri, (err) => {
      assert.ok(err);
      done();
    });
  });

  it("handles close() errors gracefully", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(`duckdb://${testDbPath}?table=points`);

    new DuckDBSource(uri, (err, source) => {
      assert.ifError(err);

      // Mock closeSync to throw an error
      const originalClose = source.connection.closeSync;
      source.connection.closeSync = () => {
        throw new Error("Mock close error");
      };

      source.close((err) => {
        // Should pass error to callback instead of throwing
        assert.ok(err, "should receive error in callback");
        assert.match(err.message, /mock close error/i);

        // Restore and cleanup
        source.connection.closeSync = originalClose;
        source.close(done);
      });
    });
  });
});

describe("DuckDBSource.getInfo", () => {
  const fixturesDir = path.join(__dirname, "fixtures");
  const infoDbPath = path.join(fixturesDir, "info.db");

  before(async () => {
    fs.mkdirSync(fixturesDir, { recursive: true });

    if (fs.existsSync(infoDbPath)) {
      fs.unlinkSync(infoDbPath);
    }

    const instance = await DuckDBInstance.create(infoDbPath);
    const conn = await instance.connect();

    await conn.run("INSTALL spatial; LOAD spatial;");
    await conn.run(`
      CREATE TABLE points (
        id INTEGER,
        name VARCHAR,
        geometry GEOMETRY
      )
    `);
    // Points in Web Mercator (EPSG:3857)
    // 0,0 in 3857 is 0,0 in 4326
    // 1000000,1000000 in 3857 is roughly 8.98, 8.95 in 4326
    await conn.run(`
      INSERT INTO points VALUES
        (1, 'Point A', ST_GeomFromText('POINT(0 0)')),
        (2, 'Point B', ST_GeomFromText('POINT(1000000 1000000)'))
    `);

    conn.closeSync();
  });

  after(() => {
    if (fs.existsSync(infoDbPath)) {
      fs.unlinkSync(infoDbPath);
    }
  });

  it("returns tile metadata with bounds from geometry extent", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(`duckdb://${infoDbPath}?table=points`);

    new DuckDBSource(uri, (err, source) => {
      assert.ifError(err);

      source.getInfo((err, info) => {
        assert.ifError(err);
        assert.strictEqual(info.format, "pbf");
        assert.strictEqual(info.minzoom, 0);
        assert.strictEqual(info.maxzoom, 14);
        assert.ok(Array.isArray(info.bounds), "bounds should be array");
        assert.strictEqual(info.bounds.length, 4);
        // Bounds should be in EPSG:4326
        assert.ok(info.bounds[0] >= -180 && info.bounds[0] <= 180);
        assert.ok(info.bounds[2] >= -180 && info.bounds[2] <= 180);
        source.close(done);
      });
    });
  });

  it("calculates center from bounds", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(`duckdb://${infoDbPath}?table=points`);

    new DuckDBSource(uri, (err, source) => {
      assert.ifError(err);

      source.getInfo((err, info) => {
        assert.ifError(err);

        // Should have center calculated from bounds
        assert.ok(info.center, "center should be defined");
        assert.ok(Array.isArray(info.center), "center should be array");
        assert.strictEqual(info.center.length, 3, "center should be [lon, lat, zoom]");

        const [minLon, minLat, maxLon, maxLat] = info.bounds;
        const [centerLon, centerLat, zoom] = info.center;

        // Center should be midpoint of bounds
        const expectedLon = (minLon + maxLon) / 2;
        const expectedLat = (minLat + maxLat) / 2;

        assert.ok(
          Math.abs(centerLon - expectedLon) < 0.0001,
          `center lon ${centerLon} should be near ${expectedLon}`
        );
        assert.ok(
          Math.abs(centerLat - expectedLat) < 0.0001,
          `center lat ${centerLat} should be near ${expectedLat}`
        );
        assert.strictEqual(typeof zoom, "number", "zoom should be a number");

        source.close(done);
      });
    });
  });

  it("handles empty tables gracefully", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const emptyDbPath = path.join(fixturesDir, "empty.db");

    // Clean up if exists from previous run
    if (fs.existsSync(emptyDbPath)) {
      fs.unlinkSync(emptyDbPath);
    }

    // Create database with empty table
    const { DuckDBInstance } = require("@duckdb/node-api");
    DuckDBInstance.create(emptyDbPath)
      .then((instance) => instance.connect())
      .then((conn) =>
        conn
          .run("INSTALL spatial; LOAD spatial;")
          .then(() =>
            conn.run(`
              CREATE TABLE empty_points (
                id INTEGER,
                geometry GEOMETRY
              )
            `)
          )
          .then(() => {
            conn.closeSync();

            const uri = new URL(`duckdb://${emptyDbPath}?table=empty_points`);
            new DuckDBSource(uri, (err, source) => {
              assert.ifError(err);

              source.getInfo((err, info) => {
                assert.ok(err, "should return error for empty table");
                assert.match(
                  err.message,
                  /no data|empty|bounds/i,
                  "should mention empty table or bounds issue"
                );
                source.close(() => {
                  fs.unlinkSync(emptyDbPath);
                  done();
                });
              });
            });
          })
      )
      .catch(done);
  });
});

describe("DuckDBSource.getTile", () => {
  const fixturesDir = path.join(__dirname, "fixtures");
  const mvtDbPath = path.join(fixturesDir, "mvt.db");

  before(async () => {
    fs.mkdirSync(fixturesDir, { recursive: true });

    if (fs.existsSync(mvtDbPath)) {
      fs.unlinkSync(mvtDbPath);
    }

    const instance = await DuckDBInstance.create(mvtDbPath);
    const conn = await instance.connect();

    await conn.run("INSTALL spatial; LOAD spatial;");
    await conn.run(`
      CREATE TABLE buildings (
        id INTEGER,
        name VARCHAR,
        geometry GEOMETRY
      )
    `);

    // Insert a point at 0,0 in Web Mercator
    await conn.run(`
      INSERT INTO buildings VALUES
        (1, 'Building A', ST_GeomFromText('POINT(0 0)'))
    `);

    conn.closeSync();
  });

  after(() => {
    if (fs.existsSync(mvtDbPath)) {
      fs.unlinkSync(mvtDbPath);
    }
  });

  it("returns MVT buffer for tile with data", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(`duckdb://${mvtDbPath}?table=buildings`);

    new DuckDBSource(uri, (err, source) => {
      assert.ifError(err);

      // Tile 0/0/0 covers the whole world, should contain our point at 0,0
      source.getTile(0, 0, 0, (err, data, headers) => {
        assert.ifError(err);
        assert.ok(Buffer.isBuffer(data), "data should be a Buffer");
        assert.ok(data.length > 0, "data should not be empty");
        assert.strictEqual(
          headers["Content-Type"],
          "application/vnd.mapbox-vector-tile"
        );
        source.close(done);
      });
    });
  });

  it("returns empty buffer for tile with no data", (_, done) => {
    const tilelive = { protocols: {} };
    const DuckDBSource = require("../index.js")(tilelive);

    const uri = new URL(`duckdb://${mvtDbPath}?table=buildings`);

    new DuckDBSource(uri, (err, source) => {
      assert.ifError(err);

      // Tile at high zoom far from origin should be empty
      source.getTile(14, 16383, 16383, (err, data, headers) => {
        assert.ifError(err);
        assert.ok(Buffer.isBuffer(data));
        source.close(done);
      });
    });
  });
});
