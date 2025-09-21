# Node.js KV Store Tests

This directory contains comprehensive tests for the Node.js KV Store web interface.

## Test Structure

### Test Files

- **`api.test.js`** - Tests for basic REST API endpoints (`/api/keys`, `/api/key/:key`, `/api/ping`, etc.)
- **`diagnostic.test.js`** - Tests for diagnostic and cluster monitoring endpoints (`/api/cluster/*`)
- **`integration.test.js`** - Integration tests for end-to-end workflows and Thrift connectivity
- **`error-handling.test.js`** - Edge cases, error conditions, and input validation tests
- **`setup.js`** - Test environment setup and configuration

### Test Coverage

The tests cover:

#### API Endpoints
- ✅ `GET /api/ping` - Server connectivity testing
- ✅ `GET /api/keys` - Key listing with pagination and filtering
- ✅ `GET /api/key/:key` - Individual key retrieval
- ✅ `POST /api/key` - Key creation and updates
- ✅ `DELETE /api/key/:key` - Key deletion
- ✅ `DELETE /api/admin/clear-all` - Bulk data clearing
- ✅ `GET /api/admin/current-endpoint` - Configuration retrieval
- ✅ `POST /api/admin/update-endpoint` - Configuration updates

#### Diagnostic Endpoints
- ✅ `GET /api/cluster/health` - Cluster health monitoring
- ✅ `GET /api/cluster/stats` - Database statistics
- ✅ `GET /api/cluster/nodes` - Node information
- ✅ `GET /api/cluster/nodes/:nodeId` - Individual node details
- ✅ `GET /api/cluster/replication` - Replication status

#### Integration Testing
- ✅ Thrift connection management
- ✅ End-to-end CRUD operations
- ✅ Bulk operations and pagination
- ✅ Buffer value conversion
- ✅ Configuration management
- ✅ Real-time diagnostic data

#### Error Handling
- ✅ Input validation and sanitization
- ✅ Special characters and Unicode support
- ✅ Connection failures and timeouts
- ✅ Malformed responses
- ✅ Rate limiting
- ✅ Concurrent request handling
- ✅ Edge cases and boundary conditions

## Running Tests

### Prerequisites

1. Install dependencies:
   ```bash
   npm install
   ```

### Test Commands

```bash
# Run all tests
npm test

# Run tests in watch mode (for development)
npm run test:watch

# Run tests with coverage report
npm run test:coverage
```

### Test Environment

The tests use mocked Thrift connections to avoid requiring a running Thrift server. This allows for:

- Fast test execution
- Reliable test results
- Testing of error conditions
- Isolated unit testing

### Mocking Strategy

- **Thrift Client**: Fully mocked using Jest mocks
- **Network Calls**: Simulated with controllable responses
- **Error Conditions**: Artificially triggered for comprehensive testing
- **Buffer Handling**: Tests both binary and string data scenarios

## Test Configuration

Tests are configured via Jest in `package.json`:

- **Environment**: Node.js
- **Timeout**: 10 seconds (for integration tests)
- **Setup**: Automatic via `setup.js`
- **Coverage**: Includes `server.js`, excludes test files and dependencies

## Coverage Reports

Coverage reports are generated in the `coverage/` directory:

- **HTML Report**: `coverage/lcov-report/index.html`
- **LCOV Data**: `coverage/lcov.info`
- **Text Summary**: Displayed in terminal

## Adding New Tests

When adding new API endpoints or functionality:

1. Add unit tests to the appropriate test file
2. Add integration tests if the feature involves multiple components
3. Add error handling tests for edge cases
4. Update this README if new test categories are added

## Debugging Tests

To debug failing tests:

1. Run individual test files: `npx jest api.test.js`
2. Use `--verbose` flag for detailed output
3. Check mock implementations match expected API behavior
4. Verify test data matches actual Thrift response formats
