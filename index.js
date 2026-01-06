"use strict";

const { DuckDBInstance } = require("@duckdb/node-api");
const path = require("path");
const zlib = require("zlib");

const DEFAULT_CENTER_ZOOM = 12;

module.exports = function (tilelive, options) {
  const DuckDBSource = function (uri, callback) {
    // Preserve original URI string to check for relative paths
    let originalUri;
    if (typeof uri === "string") {
      originalUri = uri;
    } else if (uri && uri.href) {
      // Legacy url.parse() object - extract original from href
      originalUri = uri.href;
    } else {
      originalUri = null;
    }

    // Handle both string URIs and pre-parsed objects (from tilelive)
    let parsedUri;
    if (typeof uri === "string") {
      parsedUri = new URL(uri);
    } else if (uri instanceof URL) {
      parsedUri = uri;
    } else {
      // Legacy url.parse() object from tilelive
      parsedUri = {
        pathname: uri.pathname,
        searchParams: new URLSearchParams(uri.query),
      };
    }

    // Extract path from URI, handling relative paths
    let dbPath;

    // Check original string for relative path (before URL normalization)
    if (originalUri) {
      // Strip any protocol prefix (like xray+) to get to duckdb:// part
      const pathMatch = originalUri.match(/(?:^|[+])duckdb:\/\/([^?]+)/);
      if (pathMatch) {
        const rawPath = pathMatch[1];
        if (rawPath.startsWith("./") || rawPath.startsWith("../")) {
          // Relative path - resolve from cwd
          dbPath = path.resolve(rawPath);
        } else if (rawPath.startsWith("/")) {
          // Absolute path
          dbPath = rawPath;
        } else {
          // Could be hostname:port/path or just a filename
          // If it contains /, it's likely hostname/path which we don't support
          // Otherwise treat as relative filename
          dbPath = rawPath.includes("/") ? parsedUri.pathname : path.resolve(rawPath);
        }
      } else {
        dbPath = parsedUri.pathname;
      }
    } else {
      dbPath = parsedUri.pathname;
    }

    this.dbPath = dbPath;

    const params = parsedUri.searchParams || new URLSearchParams();
    this.table = params.get("table");
    this.geometryColumn = params.get("geometry") || "geometry";
    this.layerName = params.get("layer") || this.table;

    if (!this.table) {
      return setImmediate(
        callback,
        new Error("table parameter is required in URI")
      );
    }

    this._initialize()
      .then(() => callback(null, this))
      .catch((err) => callback(err));
  };

  DuckDBSource.prototype._initialize = async function () {
    // Validate table name contains only safe characters (alphanumeric and underscore)
    if (!/^[a-zA-Z0-9_]+$/.test(this.table)) {
      throw new Error(`Invalid table name: ${this.table} (only alphanumeric and underscore allowed)`);
    }

    // Validate geometry column name contains only safe characters
    if (!/^[a-zA-Z0-9_]+$/.test(this.geometryColumn)) {
      throw new Error(`Invalid column name: ${this.geometryColumn} (only alphanumeric and underscore allowed)`);
    }

    this.instance = await DuckDBInstance.create(this.dbPath, {
      access_mode: "READ_ONLY",
    });
    this.connection = await this.instance.connect();
    await this.connection.run("LOAD spatial");

    // Verify table exists in database
    const tableCheckQuery = `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_name = ?
    `;
    const result = await this.connection.runAndReadAll(tableCheckQuery, [this.table]);
    const rows = result.getRows();

    if (rows.length === 0) {
      throw new Error(`Table does not exist: ${this.table}`);
    }

    // Verify geometry column exists in table
    const columnCheckQuery = `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = ? AND column_name = ?
    `;
    const colResult = await this.connection.runAndReadAll(columnCheckQuery, [this.table, this.geometryColumn]);
    const colRows = colResult.getRows();

    if (colRows.length === 0) {
      throw new Error(`Geometry column does not exist: ${this.geometryColumn}`);
    }
  };

  DuckDBSource.prototype.getInfo = function (callback) {
    if (this._info) {
      return setImmediate(callback, null, this._info);
    }

    // Query bounds by transforming extent from EPSG:3857 to EPSG:4326
    // Use always_xy to ensure longitude, latitude (X, Y) order
    const query = `
      SELECT
        ST_XMin(extent) as minx,
        ST_YMin(extent) as miny,
        ST_XMax(extent) as maxx,
        ST_YMax(extent) as maxy
      FROM (
        SELECT ST_Extent(ST_Transform(${this.geometryColumn}, 'EPSG:3857', 'EPSG:4326', always_xy := true)) as extent
        FROM "${this.table}"
      )
    `;

    this.connection
      .runAndReadAll(query)
      .then((reader) => {
        const rows = reader.getRows();

        if (rows.length === 0 || !rows[0]) {
          throw new Error("No data found in table");
        }

        const row = rows[0];

        // Check for null bounds (empty table or all NULL geometries)
        if (row.some((v) => v === null || v === undefined)) {
          throw new Error("Unable to calculate bounds - table may be empty or contain invalid geometry data");
        }

        const bounds = [row[0], row[1], row[2], row[3]];

        // Calculate center from bounds
        const centerLon = (bounds[0] + bounds[2]) / 2;
        const centerLat = (bounds[1] + bounds[3]) / 2;

        this._info = {
          bounds: bounds,
          center: [centerLon, centerLat, DEFAULT_CENTER_ZOOM],
          minzoom: 0,
          maxzoom: 14,
          format: "pbf",
          vector_layers: [
            {
              id: this.layerName,
              fields: {}, // Will be populated from actual tile data
            },
          ],
        };
        callback(null, this._info);
      })
      .catch((err) => callback(err));
  };

  DuckDBSource.prototype.getTile = function (z, x, y, callback) {
    const headers = {
      "Content-Type": "application/vnd.mapbox-vector-tile",
    };

    // First, get column names (excluding geometry) for MVT properties
    this._getColumns()
      .then((propertyColumns) => {
        // Build struct fields for ST_AsMVT
        // ST_Reverse corrects polygon winding for tilelive-vector's strict MVT v2 validation
        // (ST_AsMVTGeom's Y-axis flip from Web Mercator → tile coords reverses winding)
        // ST_Extent converts GEOMETRY to BOX_2D type required by ST_AsMVTGeom
        const geometryField = `"geometry": ST_Reverse(ST_AsMVTGeom(
          t.${this.geometryColumn},
          ST_Extent(ST_TileEnvelope(${z}, ${x}, ${y}))
        ))`;

        const propertyFields = propertyColumns.length > 0
          ? ", " + propertyColumns.map((c) => `"${c}": t."${c}"`).join(", ")
          : "";

        const mvtQuery = `
          SELECT ST_AsMVT({${geometryField}${propertyFields}}, '${this.layerName}') as mvt
          FROM "${this.table}" t
          WHERE ST_Intersects(t.${this.geometryColumn}, ST_TileEnvelope(${z}, ${x}, ${y}))
        `;

        return this.connection.runAndReadAll(mvtQuery);
      })
      .then((reader) => {
        const rows = reader.getRows();
        const blob = rows[0]?.[0];
        // DuckDB returns blob as DuckDBBlobValue with bytes property
        const mvtData = blob?.bytes || Buffer.alloc(0);

        // Gzip compress for tilelive-vector compatibility
        zlib.gzip(mvtData, (err, compressed) => {
          if (err) return callback(err);
          callback(null, compressed, headers);
        });
      })
      .catch((err) => callback(err));
  };

  DuckDBSource.prototype._getColumns = async function () {
    if (this._columns) {
      return this._columns;
    }

    const columnsQuery = `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = '${this.table}'
        AND column_name != '${this.geometryColumn}'
    `;

    const reader = await this.connection.runAndReadAll(columnsQuery);
    const rows = reader.getRows();
    this._columns = rows.map((r) => r[0]);
    return this._columns;
  };

  DuckDBSource.prototype.close = function (callback) {
    try {
      if (this.connection) {
        this.connection.closeSync();
        this.connection = null;
      }
      if (this.instance) {
        this.instance = null;
      }
      return callback && setImmediate(callback);
    } catch (err) {
      return callback && setImmediate(callback, err);
    }
  };

  DuckDBSource.registerProtocols = function (tilelive) {
    tilelive.protocols["duckdb:"] = DuckDBSource;
  };

  DuckDBSource.registerProtocols(tilelive);

  return DuckDBSource;
};
