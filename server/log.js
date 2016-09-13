const _ = require('lodash');
const config = require('./config');
const rollbar = require('rollbar');
rollbar.init(config.rollbar.token);
rollbar.handleUnhandledRejections(config.rollbar.token);

/**
 * Function that send an info log message to Rollbar.
 * 
 * @param {object} message - The log message.
 * @param {object} object - The log object.
 * @param {object} request - The HTTP request, in case this is logged based on a HTTP request.
 * @param {object} cb - The callback function to call after the log has been logged.
 */
const info = (message, object, request, cb) => {
  log('info', message, object, request, cb);
};

/**
 * Function that send a warning log message to Rollbar.
 * 
 * @param {object} message - The log message.
 * @param {object} object - The log object.
 * @param {object} request - The HTTP request, in case this is logged based on a HTTP request.
 * @param {object} cb - The callback function to call after the log has been logged.
 */
const warn = (message, object, request, cb) => {
  log('warn', message, object, request, cb);
};

/**
 * Function that send an error log message to Rollbar.
 * 
 * @param {object} message - The log message.
 * @param {object} object - The log object.
 * @param {object} request - The HTTP request, in case this is logged based on a HTTP request.
 * @param {object} cb - The callback function to call after the log has been logged.
 */
const error = (message, object, request, cb) => {
  log('error', message, object, request, cb);
};

// ------------------- HELPER FUNCTIONS -------------------

/**
 * Function that send a log message to Rollbar.
 * 
 * @param {object} level - The log level (info, warn, error).
 * @param {object} message - The log message.
 * @param {object} object - The log object.
 * @param {object} request - The HTTP request, in case this is logged based on a HTTP request.
 * @param {object} cb - The callback function to call after the log has been logged.
 */
const log = (level, message, object, request, cb) => {
  if (_.isUndefined(cb) && !_.isFunction(cb)) {
    cb = function () {};
  }
  if (!_.isUndefined(message) && !_.isUndefined(object)) {
    rollbar.reportMessageWithPayloadData(message, {
      level: level,
      custom: object
    }, request, cb);
  } else {
    cb();
  }
};

// ------------------- RETURNED OBJECT -------------------
module.exports = {
  info: info,
  warn: warn,
  error: error
};
