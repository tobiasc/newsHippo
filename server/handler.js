'use strict';

const config = require('./config');
const log = require('./log');
const AWS = require('aws-sdk');
const awsConfig = config.aws;
const dynamodb = new AWS.DynamoDB.DocumentClient(awsConfig);
var sns = new AWS.SNS(awsConfig);
const _ = require('lodash');
const async = require('async');
const url = require('url');
const havenondemand = require('havenondemand');
const client = new havenondemand.HODClient(config.haven.token, 'v1');

/**
 * Function that add an article to the db, and if the url is unknown, adds that.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const articleCreate = (event, context, cb) => {
  /**
   * - Parse input parameters
   * - Verify input parameters
   * - If article does not exist, add it
   * - If newsSource does not exist, add it
   * - Publish to SNS for further processing
   * - Return
   */
  let params = parseInputHTTP(event);

  if (!_.isUndefined(params.url)) {
    try {
      const u = new url.parse(params.url);
      let urlChanged = false;

      async.waterfall([
        // if article does not exist, add it
        function (cb1) {
          const query = {
            Key: {
              url: params.url
            },
            TableName: 'article'
          };
          dynamodb.get(query, function(err, data) {
            if (err) {
              cb1(err);
            } else {
              if (_.isUndefined(data) || _.isUndefined(data.Item)) {
                const putQuery = {
                  Item: {
                    url: params.url,
                    newsSource: u.hostname
                  },
                  TableName: 'article'
                };
                dynamodb.put(putQuery, function(err, data) {
                  if (err) {
                    cb1(err);
                  } else {
                    urlChanged = true;
                    cb1();
                  }
                });
              } else {
                cb1();
              }
            }
          });
        },

        // if newsSource does not exist, add it
        function (cb1) {
          const query = {
            Key: {
              url: u.hostname
            },
            TableName: 'newsSource'
          };
          dynamodb.get(query, function(err, data) {
            if (err) {
              cb1(err);
            } else {
              if (_.isUndefined(data) || _.isUndefined(data.Item)) {
                const putQuery = {
                  Item: {
                    url: u.hostname
                  },
                  TableName: 'newsSource'
                };
                dynamodb.put(putQuery, function(err, data) {
                  cb1(err);
                });
              } else {
                cb1();
              }
            }
          });
        },

        // publish to SNS
        function (cb1) {
          let snsParams = {
            Message: 'Process new URL',
            MessageAttributes: {
              url: {
                DataType: 'String',
                StringValue: params.url
              }
            }
          };

          async.waterfall([
            // textConcepts
            function (cb2) {
              snsParams.TopicArn = 'arn:aws:sns:us-east-1:196351684507:newsHippo-textConcepts';
              sns.publish(snsParams, function(err, data) {
                cb2(err);
              });
            }, 

            // getTextLanguage
            function (cb2) {
              snsParams.TopicArn = 'arn:aws:sns:us-east-1:196351684507:newsHippo-textLanguage';
              sns.publish(snsParams, function(err, data) {
                cb2(err);
              });
            },

            // getTextSentiment
            function (cb2) {
              snsParams.TopicArn = 'arn:aws:sns:us-east-1:196351684507:newsHippo-textSentiment';
              sns.publish(snsParams, function(err, data) {
                cb2(err);
              });
            }, 

            // getTextStats
            function (cb2) {
              snsParams.TopicArn = 'arn:aws:sns:us-east-1:196351684507:newsHippo-textStats';
              sns.publish(snsParams, function(err, data) {
                cb2(err);
              });
            }

          ], function (err) {
            cb1(err);
          });
        }

      ], function (err) {
        if (err) {
          sendResponse(err, {
            status: 'error',
            error: err
          }, null, 'articleCreate', event, cb);
        } else {
          sendResponse(null, {
            status: 'success'
          }, null, 'articleCreate', event, cb);
        }
      });
    } catch (err) {
      sendResponse(err, {
        status: 'error',
        error: err
      }, null, 'articleCreate', event, cb);
    }

  } else {
    sendResponse(new Error('Missing parameters'), {
      status: 'error',
      error: 'Missing parameters'
    }, null, 'articleCreate', event, cb);
  }
};

/**
 * Function that deletes an article from the db.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const articleDelete = (event, context, cb) => {
  /**
   * - Parse input parameters
   * - Verify input parameters
   * - If article does exist, delete it
   * - Return
   */
  let params = parseInputHTTP(event);

  if (!_.isUndefined(params.url)) {
    try {
      const u = new url.parse(params.url);

      // if article does not exist, add it
      const query = {
        Key: {
          url: params.url
        },
        TableName: 'article'
      };
      dynamodb.delete(query, function(err, data) {
        if (err) {
          sendResponse(err, {
            status: 'error',
            error: err
          }, null, 'articleDelete', event, cb);
        } else {
          sendResponse(null, {
            status: 'success'
          }, null, 'articleDelete', event, cb);
        }
      });

    } catch (err) {
      sendResponse(err, {
        status: 'error',
        error: err
      }, null, 'articleDelete', event, cb);
    }

  } else {
    sendResponse(new Error('Missing parameters'), {
      status: 'error',
      error: 'Missing parameters'
    }, null, 'articleDelete', event, cb);
  }
};

/**
 * Function that queries the db and returns the matching articles.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const articleGet = (event, context, cb) => {
  let params = parseInputHTTP(event);

  if (!_.isUndefined(params.url)) {
    try {
      const u = new url.parse(params.url);

      const query = {
        Key: {
          url: params.url
        },
        TableName: 'article'
      };
      dynamodb.get(query, function(err, data) {
        if (err) {
          sendResponse(err, {
            status: 'error',
            error: err
          }, null, 'articleGet', event, cb);
        } else {
          sendResponse(null, data, null, 'articleGet', event, cb);
        }
      });

    } catch (err) {
      sendResponse(err, {
        status: 'error',
        error: err
      }, null, 'articleGet', event, cb);
    }

  } else {
    sendResponse(new Error('Missing parameters'), {
      status: 'error',
      error: 'Missing parameters'
    }, null, 'articleGet', event, cb);
  }
};

/**
 * Function that queries the db and returns the matching newsSources.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const newsSourceList = (event, context, cb) => {
  const query = {
    TableName: 'newsSource'
  };
  dynamodb.scan(query, function(err, data) {
    if (err) {
      sendResponse(err, {
        status: 'error',
        error: err
      }, null, 'newsSourceGet', event, cb);
    } else {
      sendResponse(null, data.Items, null, 'newsSourceGet', event, cb);
    }
  });
};

/**
 * Function that finds concepts within a text or url. If they are found, they are added to the  article.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const getTextConcepts = (event, context, cb) => {
  const u = parseInputSNS(event);
  if (!_.isNull(u)) {
    const query = {
      url: u.href
    };
    client.post('extractconcepts', query).on('data', body => {
      if (!_.isUndefined(body) && !_.isUndefined(body.concepts)) {
        const params = {
            TableName: 'article',
            Key: {
              url: u.href
            },
            UpdateExpression: 'set concepts = :r',
            ExpressionAttributeValues: {
                ':r': body.concepts
            }
        };
        dynamodb.update(params, function(err, data) {
          if (err) {
            sendResponse(err, {
              status: 'error',
              error: err
            }, null, 'getTextConcepts', event, cb);
          } else {
            sendResponse(null, {
              status: 'success'
            }, null, 'getTextConcepts', event, cb);
          }
        });
      } else {
        sendResponse(new Error('No concepts found'), {
          status: 'error',
          error: 'No concepts found'
        }, null, 'getTextConcepts', event, cb);
      }
    });
  } else {
    sendResponse(new Error('Bad input'), {
      status: 'error',
      error: 'Bad input'
    }, null, 'getTextConcepts', event, cb);
  }
};

/**
 * Function that finds the language within a text or url. If found, it is added to the article.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const getTextLanguage = (event, context, cb) => {
  const u = parseInputSNS(event);
  if (!_.isNull(u)) {
    const query = {
      url: u.href
    };
    client.post('identifylanguage', query).on('data', body => {
      if (!_.isUndefined(body) && !_.isUndefined(body.language)) {
        const params = {
            TableName: 'article',
            Key: {
              url: u.href
            },
            UpdateExpression: 'set lang = :r',
            ExpressionAttributeValues: {
                ':r': body.language
            }
        };
        dynamodb.update(params, function(err, data) {
          if (err) {
            sendResponse(err, {
              status: 'error',
              error: err
            }, null, 'getTextLanguage', event, cb);
          } else {
            sendResponse(null, {
              status: 'success'
            }, null, 'getTextLanguage', event, cb);
          }
        });
      } else {
        sendResponse(new Error('No language found'), {
          status: 'error',
          error: 'No language found'
        }, null, 'getTextLanguage', event, cb);
      }
    });
  } else {
    sendResponse(new Error('Bad input'), {
      status: 'error',
      error: 'Bad input'
    }, null, 'getTextLanguage', event, cb);
  }
};

/**
 * Function that finds the sentiment within a text or url. If found, it is added to the article.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const getTextSentiment = (event, context, cb) => {
  const u = parseInputSNS(event);
  if (!_.isNull(u)) {
    const query = {
      url: u.href
    };
    client.post('analyzesentiment', query).on('data', body => {
      if (!_.isUndefined(body) && !_.isUndefined(body.aggregate) && !_.isUndefined(body.aggregate.sentiment)) {
        const params = {
            TableName: 'article',
            Key: {
              url: u.href
            },
            UpdateExpression: 'set sentiment = :r',
            ExpressionAttributeValues: {
                ':r': body.aggregate
            }
        };
        dynamodb.update(params, function(err, data) {
          if (err) {
            sendResponse(err, {
              status: 'error',
              error: err
            }, null, 'getTextSentiment', event, cb);
          } else {
            sendResponse(null, {
              status: 'success'
            }, null, 'getTextSentiment', event, cb);
          }
        });
      } else {
        sendResponse(new Error('No sentiment found'), {
          status: 'error',
          error: 'No sentiment found'
        }, null, 'getTextSentiment', event, cb);
      }
    });
  } else {
    sendResponse(new Error('Bad input'), {
      status: 'error',
      error: 'Bad input'
    }, null, 'getTextSentiment', event, cb);
  }
};

/**
 * Function that finds statistics around a text or url. If found, they are added to the article.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const getTextStats = (event, context, cb) => {
  const u = parseInputSNS(event);
  if (!_.isNull(u)) {
    const query = {
      url: u.href
    };
    client.post('gettextstatistics', query).on('data', body => {
      if (!_.isUndefined(body) && !_.isUndefined(body.sentences)) {
        const params = {
            TableName: 'article',
            Key: {
              url: u.href
            },
            UpdateExpression: 'set statistics = :r',
            ExpressionAttributeValues: {
                ':r': body
            }
        };
        dynamodb.update(params, function(err, data) {
          if (err) {
            sendResponse(err, {
              status: 'error',
              error: err
            }, null, 'getTextStats', event, cb);
          } else {
            sendResponse(null, {
              status: 'success'
            }, null, 'getTextStats', event, cb);
          }
        });
      } else {
        sendResponse(new Error('No statistics found'), {
          status: 'error',
          error: 'No statistics found'
        }, null, 'getTextStats', event, cb);
      }
    });
  } else {
    sendResponse(new Error('Bad input'), {
      status: 'error',
      error: 'Bad input'
    }, null, 'getTextStats', event, cb);
  }
};

/**
 * Function that finds labels from images. If found, they are added to the article.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const getImageLabels = (event, context, cb) => cb(null,
  { message: 'Hello getImageLabels', event }
);

/**
 * Function that determines whether an image is explicit. If it is, that is added to the  article.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} context - The context of the Lambda function.
 * @param {object} cb - The callback function that returns the Lambda function.
 */
const getImageExplicit = (event, context, cb) => cb(null,
  { message: 'Hello getImageExplicit', event }
);

// ------------------- HELPER FUNCTIONS -------------------

/**
 * Function that merges the HTTP body and/or HTTP query objects from the input event to Lambda and returns that.
 * 
 * @param {object} event - The event the Lambda function was called with from a HTTP call.
 * @return {object} params - The extracted body or query variables.
 */
const parseInputHTTP = event => {
  const params = {};
  if (!_.isUndefined(event.body)) {
    _.merge(params, event.body);
  }
  if (!_.isUndefined(event.query)) {
    _.merge(params, event.query);
  }
  return params;
};

/**
 * Function that merges the body and query objects from the input event and returns that.
 * 
 * @param {object} event - The event the Lambda function was called with.
 * @return {object} params - The extracted body or query variables.
 */
const parseInputSNS = event => {
  let u = null;
  if (!_.isUndefined(event) && !_.isUndefined(event.Records[0]) && !_.isUndefined(event.Records[0].Sns) && 
      !_.isUndefined(event.Records[0].Sns.MessageAttributes) && !_.isUndefined(event.Records[0].Sns.MessageAttributes.url) &&
      !_.isUndefined(event.Records[0].Sns.MessageAttributes.url.Value)) {
    try {
      u = url.parse(event.Records[0].Sns.MessageAttributes.url.Value);
    } catch (err) {

    }
  }
  return u;
};

/**
 * Function send the Lambda response and logs it.
 * 
 * @param {object} err - The error we need to return.
 * @param {object} data - The return message.
 * @param {object} req - The HTTP request object.
 * @param {object} source - The function that called this.
 * @param {object} event - The event the Lambda function was called with.
 * @param {object} cb - The callback function to call after the log has been logged.
 */
const sendResponse = (err, data, req, source, event, cb) => {
  const obj = {
    data: data,
    source: source,
    event: event
  };

  const logFunc = (err) ? log.error : log.info;
  logFunc('Request handled by ' + source, obj, req, function () {
    cb(null, data);
  });
};

// ------------------- RETURNED OBJECT -------------------
const funcs = {
  articleCreate: articleCreate,
  articleDelete: articleDelete,
  articleGet: articleGet,
  newsSourceList: newsSourceList,
  getTextConcepts: getTextConcepts,
  getTextLanguage: getTextLanguage,
  getTextSentiment: getTextSentiment,
  getTextStats: getTextStats,
  getImageLabels: getImageLabels,
  getImageExplicit: getImageExplicit
};

module.exports = funcs; 

// var evt = {
//   "Records": [
//     {
//       "EventVersion": "1.0",
//       "EventSubscriptionArn": "arn:aws:sns:EXAMPLE",
//       "EventSource": "aws:sns",
//       "Sns": {
//         "SignatureVersion": "1",
//         "Timestamp": "1970-01-01T00:00:00.000Z",
//         "Signature": "EXAMPLE",
//         "SigningCertUrl": "EXAMPLE",
//         "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
//         "Message": "Hello from SNS!",
//         "MessageAttributes": {
//           "url": {
//             "Type": "String",
//             "Value": "http://www.businessinsider.com/chance-the-rapper-buys-scalper-tickets-to-his-festival-sells-to-fans-2016-9"
//           }
//         },
//         "Type": "Notification",
//         "UnsubscribeUrl": "EXAMPLE",
//         "TopicArn": "arn:aws:sns:EXAMPLE",
//         "Subject": "TestInvoke"
//       }
//     }
//   ]
// };
// getTextExtract(evt, null, console.log);
// addArticle({query:{url: 'http://www.businessinsider.com/chance-the-rapper-buys-scalper-tickets-to-his-festival-sells-to-fans-2016-9'}}, null, console.log);
