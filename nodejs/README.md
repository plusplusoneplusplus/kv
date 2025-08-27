# KV Store Web Viewer

A simple Node.js web interface for viewing and managing key-value pairs in the Thrift RocksDB server.

## Features

- View all key-value pairs with pagination
- Search for specific keys
- Add new key-value pairs
- Edit existing values
- Delete keys
- Connection status monitoring
- Real-time server ping testing

## Prerequisites

1. Node.js (v14 or later)
2. Thrift RocksDB server running (default port: 9090)

## Installation

1. Generate Thrift JavaScript files:
```bash
# From the project root directory
thrift --gen js:node thrift/kvstore.thrift
cp gen-nodejs/* nodejs/thrift/
```

2. Install Node.js dependencies:
```bash
cd nodejs
npm install
```

## Running the Web Server

```bash
npm start
```

The web interface will be available at http://localhost:3000

## Configuration

Environment variables:
- `PORT`: Web server port (default: 3000)
- `THRIFT_HOST`: Thrift server host (default: localhost)
- `THRIFT_PORT`: Thrift server port (default: 9090)

Example:
```bash
PORT=8080 THRIFT_HOST=192.168.1.100 THRIFT_PORT=9090 npm start
```

## Usage

1. Start the Thrift RocksDB server:
   ```bash
   # From the rust directory
   cargo run --bin thrift-server
   ```

2. Start the web server:
   ```bash
   # From the nodejs directory
   npm start
   ```

3. Open your browser to http://localhost:3000

## API Endpoints

- `GET /api/keys` - Get all key-value pairs (supports ?limit and ?startKey)
- `GET /api/key/:key` - Get a specific key
- `POST /api/key` - Create or update a key-value pair
- `DELETE /api/key/:key` - Delete a key
- `GET /api/ping` - Test server connection

## Interface Features

- **Connection Status**: Shows real-time connection status with the Thrift server
- **Key Management**: Add, edit, and delete key-value pairs through a user-friendly interface
- **Search**: Find specific keys quickly
- **Pagination**: Handle large datasets with configurable limits
- **Value Preview**: Truncated value display with full view option
- **Responsive Design**: Works on desktop and mobile devices