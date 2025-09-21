// Test setup file
// This file is run before all tests to set up the testing environment

// Mock console.log to reduce noise during testing
global.console = {
  ...console,
  log: jest.fn(),
  error: jest.fn(), // Mock error logging too to reduce noise
  warn: console.warn,
  info: console.info,
  debug: console.debug,
};

// Set test environment variables
process.env.NODE_ENV = 'test';
process.env.PORT = '0'; // Use random port for testing
process.env.THRIFT_HOST = 'localhost';
process.env.THRIFT_PORT = '9090';

// Increase timeout for integration tests
jest.setTimeout(10000);

// Clear all timers after each test to prevent leaks
afterEach(() => {
  jest.clearAllTimers();
});
