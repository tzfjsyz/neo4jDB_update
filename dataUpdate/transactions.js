/*
用于neo4j的数据导入、更新、建立索引、路径查询等等
wrote by tzf, 2017/12/27
*/
const neo4j = require('neo4j-driver').v1;
const req = require('require-yml');
const config = req('./config/source.yml');
const log4js = require('log4js');

// Transaction functions provide a convenient API with minimal boilerplate and
// retries on network fluctuations and transient errors. Maximum retry time is
// configured on the driver level and is 30 seconds by default:
const driver_a = neo4j.driver(`${config.neo4jServer_a.url}`, neo4j.auth.basic(`${config.neo4jServer_a.user}`, `${config.neo4jServer_a.password}`), { maxTransactionRetryTime: 30000 });
const driver_b = neo4j.driver(`${config.neo4jServer_b.url}`, neo4j.auth.basic(`${config.neo4jServer_b.user}`, `${config.neo4jServer_b.password}`), { maxTransactionRetryTime: 30000 });
const Redis = require('ioredis');
const redis = new Redis(config.redisUrl[0]);
const pubClient = new Redis(config.redisPubSubInfo.clientUrl[0]);
const Client = require('dict-client');
let client = new Client(config.dictionaryServer.host, config.dictionaryServer.port);
console.log('dict-client host: ' + config.dictionaryServer.host + ', port: ' + config.dictionaryServer.port);
const holder_dict_redis = new Redis(config.redisUrl[2]);
redis.on('error', function (error) {
  console.dir(error);
  console.log('redis: ',error);
  logger.info('redis: ',error);
});
holder_dict_redis.on('error', function (error) {
  console.dir(error);
  console.log('holder_dict_redis: ',error);
  logger.info('holder_dict_redis: ',error);
})

log4js.configure({
  appenders: {
    'out': {
      type: 'file',         //文件输出
      filename: 'logs/updateData.log',
      maxLogSize: config.logInfo.maxLogSize
    }
  },
  categories: { default: { appenders: ['out'], level: 'info' } }
});

const logger = log4js.getLogger();

let transactions = {

  //通过ITCode2查询是不是分支机构
  judgeBranches: async function (code) {
    return new Promise((resolve, reject) => {
      let now = Date.now();
      client.batchLookup('BRANCHES', code)
        .then(res => {
          let judgeBranchesCost = Date.now() - now;
          console.log('judgeBranchesCost: ', +judgeBranchesCost + 'ms');
          if (res.length == 0) {
            return resolve(null);
          } else if (res.length > 0) {
            return resolve(res);
          }
        }).catch(err => {
          if (err.name === 'InvalidOperation' && err.ok == -505) {
            return resolve(null);
          } else {
            console.error(err);
            return reject(err);
          }
        });
    });
  },

  //通过ITCode2查询是不是分支机构
  /*  judgeBranches: async function (code) {
      try {
        let now = Date.now();
        // let res = await client.lookup('BRANCHES', code);
        let res = await client.batchLookup('BRANCHES', code);
        let judgeBranchesCost = Date.now() - now;
        console.log('judgeBranchesCost: ', +judgeBranchesCost + 'ms');
        if (res.length == 0) {
          return null;
        } else if (res.length > 0) {
          return res;
        }
      } catch (err) {
        if (err.name = 'InvalidOperation' && err.ok == -505) {
          return null;
        } else {
          console.error(err);
          logger.error(err);
          return err;
        }
      }
    },
  */
  //产生随机数函数
  createRndNum: function (n) {
    let rnd = "";
    for (let i = 0; i < n; i++)
      rnd += Math.floor(Math.random() * 10);
    rnd = rnd.replace('0', '1');                    //首位0用1替换
    rnd = parseInt(rnd);
    return rnd;
  },

  //通过提交POST数据进行导入
  startPostData: async function (request) {
    return new Promise(async (resolve, reject) => {
      let session = driver.session();
      let writeTxPromise = null;
      let ITCodeOne = request.codeOne;
      let ITCodeTwo = request.codeTwo;
      let ITNameOne = request.nameOne;
      let ITNameTwo = request.nameTwo;
      let holdWeight = request.weight;
      let queryBody = "";
      for (let i = 0; i < ITCodeOne.length; i++) {
        queryBody += `merge (:company { ITCode2: "${ITCodeOne[i]}", ITName: "${ITNameOne[i]}"})-[:invests {weight: ${holdWeight[i]}}]->(:company { ITCode2: "${ITCodeTwo[i]}", ITName: "${ITNameTwo[i]}"})` + '  ';
      }
      // queryBody =+' ' +'create index on :company(ITCode2)';
      writeTxPromise = session.writeTransaction(tx => tx.run(queryBody));

      writeTxPromise.then(result => {
        let updateStatus = {};
        let status = result.summary.updateStatistics._stats;
        if (status.labelsAdded != 0 && status.nodesCreated != 0 && status.propertiesSet != 0 && status.relationshipsCreated != 0) {
          updateStatus.status = 'Import data success!';
          updateStatus.labelsAdded = status.labelsAdded;
          updateStatus.nodesCreated = status.nodesCreated;
          updateStatus.propertiesSet = status.propertiesSet;
          updateStatus.relationshipsCreated = status.relationshipsCreated;
        } else {
          updateStatus.status = 'Import data failure!';
        }
        driver.close();
        session.close();
        return resolve(updateStatus);
        console.log(updateStatus);
      }).catch(function (error) {
        console.log(error);
      });
    });
  },

  //建立索引
  startCreateIndex: async function (flag, labelName, indexName) {
    return new Promise(async (resolve, reject) => {
      let driver = null;
      if (flag == 'a') driver = driver_a;
      else if (flag == 'b') driver = driver_b;
      let session = driver.session();
      if (!labelName) labelName = config.nodeLabelName1;
      if (!indexName) indexName = config.nodeIndexName1;
      let queryBody = `create index on :${labelName}(${indexName})`;
      writeTxPromise = session.writeTransaction(tx => tx.run(queryBody));
      writeTxPromise.then(result => {
        let status = result.summary.updateStatistics._stats.indexesAdded;
        // driver.close();
        session.close();
        console.log(`neo4j_${flag} indexesAdded: ` + status);
        logger.info(`neo4j_${flag} indexesAdded: ` + status);
        return resolve(status);
      }).catch(function (error) {
        console.error(error);
        logger.error(error);
        return reject(error);
      });
    });
  },

  //删除索引
  startDeleteIndex: async function (labelName, indexName) {
    return new Promise(async (resolve, reject) => {
      let session = driver.session();
      if (!labelName) labelName = config.nodeLabelName1;
      if (!indexName) indexName = config.nodeIndexName1;
      let queryBody = `drop index on :${labelName}(${indexName})`;
      writeTxPromise = session.writeTransaction(tx => tx.run(queryBody));
      writeTxPromise.then(result => {
        let status = result.summary.updateStatistics._stats;
        // driver.close();
        session.close();
        return resolve(status);
      }).catch(function (error) {
        console.error(error);
        logger.error(error);
        return reject(error);
      });
    });
  },

  //通过load CSV语句进行relation 更新数据的导入
  startLoadInreRelCSV: async function (fileName) {
    return new Promise(async (resolve, reject) => {
      let session = driver.session();
      let loadInreRelCSVInfo = {};
      let loadInreRelCSVMsg = null;
      let updateInfo = [];
      let updateMsg = [];
      let queryBody = '';
      try {
        loadInreRelCSVMsg = `import ${fileName}.csv successfully!`;

        //read csv headers
        let readCSVHeaders = ` 
                              LOAD CSV WITH HEADERS FROM "file:///${fileName}.csv" AS line
                              
                              `;

        //fromId变， toId不变
        let fromIdChangeBody = `                                  
                                  MATCH (to :company{ITCode2: toInteger(line.toId)})
                                  CREATE (from :company{ITCode2: toInteger(line.toId)}) -[r:invests]-> (to)
                                  SET r.weight = line.weight
                                  return from,r,to;
                                `;
        //fromId/toId 同时不存在
        let fromToIdSyncAbsentBody = ` 
                                      CREATE (from :company{ITCode2: toInteger(line.fromId)}) -[r:invests]-> (to :company{ITCode2: toInteger(line.toId)})  
                                      SET r.weight = line.weight
                                      return from,r,to;   
                                    `;
        //fromId/toId 同时存在
        let fromToIdSyncExistBody = ` 
                                      MATCH (from :company{ITCode2: toInteger(line.fromId)}), (to :company{ITCode2: toInteger(line.toId)})  
                                      CREATE (from) -[r:invests]-> (to)
                                      SET r.weight = line.weight
                                      return from,r,to;   
                                    `;
        //fromId不变, toId变
        let toIdChangeBody = ` 
                              MATCH (from :company{ITCode2: toInteger(line.fromId)})
                              CREATE (from) -[r:invests]-> (to :company{ITCode2: toInteger(line.toId)})
                              SET r.weight = line.weight
                              return from,r,to;
                              `;
        //relations delete
        let relationsDeleteBody = `
                                    MATCH (from :company{ITCode2: toInteger(line.fromId)}) -[r:invests]-> (to :company{ITCode2: toInteger(line.toId)})
                                    delete r;
                                  `;
        //relations modify
        let relationsModifyBody = `
                                    MATCH (from :company{ITCode2: toInteger(line.fromId)}) -[r:invests]-> (to :company{ITCode2: toInteger(line.toId)})
                                    SET r.weight = line.weight
                                    return from,r,to;
                                  `;
        //最终执行的csv 语句
        // let queryBody = `
        //                   LOAD CSV WITH HEADERS FROM "file:///incrementalRelations.csv" AS line
        //                   MATCH (to :company{ITCode2: toInteger(line.toId)})
        //                   CREATE (from :company{ITCode2: toInteger(line.toId)}) -[r:invests]-> (to)
        //                   SET r.weight = line.weight                        
        //                   WHERE line.flag = 0 and ${queryFromId(line.fromId)} = false and ${queryToId(toId)} = true
        //                   return from,r,to;  

        //                   LOAD CSV WITH HEADERS FROM "file:///incrementalRelations.csv" AS line
        //                   CREATE (from :company{ITCode2: toInteger(line.fromId)}) -[r:invests]-> (to :company{ITCode2: toInteger(line.toId)})  
        //                   SET r.weight = line.weight
        //                   WHERE line.flag = 0 and ${queryFromId(line.fromId)} = false and ${queryToId(toId)} = false
        //                   return from,r,to;   

        //                   LOAD CSV WITH HEADERS FROM "file:///incrementalRelations.csv" AS line
        //                   MATCH (from :company{ITCode2: toInteger(line.fromId)}), (to :company{ITCode2: toInteger(line.toId)})  
        //                   CREATE (from) -[r:invests]-> (to)
        //                   SET r.weight = line.weight                      
        //                   WHERE line.flag = 0 and ${queryFromId(line.fromId)} = true and ${queryToId(toId)} = true
        //                   return from,r,to;      

        //                   LOAD CSV WITH HEADERS FROM "file:///incrementalRelations.csv" AS line
        //                   MATCH (from :company{ITCode2: toInteger(line.fromId)})
        //                   CREATE (from) -[r:invests]-> (to :company{ITCode2: toInteger(line.toId)})
        //                   SET r.weight = line.weight
        //                   WHERE line.flag = 0 and ${queryFromId(line.fromId)} = true and ${queryToId(toId)} = false
        //                   return from,r,to;

        //                   LOAD CSV WITH HEADERS FROM "file:///incrementalRelations.csv" AS line
        //                   MATCH (from :company{ITCode2: toInteger(line.fromId)}) -[r:invests]-> (to :company{ITCode2: toInteger(line.toId)})
        //                   WHERE line.flag = 1
        //                   delete r;

        //                   LOAD CSV WITH HEADERS FROM "file:///incrementalRelations.csv" AS line
        //                   MATCH (from :company{ITCode2: toInteger(line.fromId)}) -[r:invests]-> (to :company{ITCode2: toInteger(line.toId)})
        //                   SET r.weight = line.weight
        //                   WHERE line.flag = 2
        //                   return from,r,to;
        //                   `;

        let queryBody = `
                          LOAD CSV WITH HEADERS FROM "file:///incrementalRelations.csv" AS line
                          MATCH (from :company{ITCode2: toInteger(line.fromId)}), (to :company{ITCode2: toInteger(line.toId)}), (from) -[r:invests]-> (to)
                          FOREACH (_ IN CASE WHEN line.flag = '0' THEN [1] ELSE [] END |
                                                    MERGE (from) -[r:invests]-> (to)
                                                    SET r.weight = line.weight
                          )
                          FOREACH (_ IN CASE WHEN line.flag = '1' THEN [1] ELSE [] END |
                                                    
                                                    DELETE r
                          )
                          FOREACH (_ IN CASE WHEN line.flag = '2' THEN [1] ELSE [] END |
                                                    
                                                    SET r.weight = line.weight
                          )
                          `;

        // if (fileName == 'fromIdChange') {
        //   queryBody = fromIdChangeBody;

        // } else if (fileName == 'fromToIdSyncAbsent') {
        //   queryBody = fromToIdSyncAbsentBody;

        // } else if (fileName == 'fromToIdSyncExist') {
        //   queryBody = fromToIdSyncExistBody;

        // } else if (fileName == 'toIdChange') {
        //   queryBody = toIdChangeBody;

        // } else if (fileName == 'relations-delete') {
        //   queryBody = relationsDeleteBody;

        // } else if (fileName == 'relations-modify') {
        //   queryBody = relationsModifyBody;

        // }

        writeTxPromise = await session.writeTransaction(tx => tx.run(queryBody));
        updateInfo.push(writeTxPromise.summary.updateStatistics._stats);
        updateMsg.push(loadInreRelCSVMsg);
        loadInreRelCSVInfo.updateInfo = updateInfo;
        console.log('labelsAdded: ' + updateInfo[0].labelsAdded + ', labelsRemoved: ' + updateInfo[0].labelsRemoved + ', nodesCreated: ' + updateInfo[0].nodesCreated
          + ', nodesDeleted: ' + updateInfo[0].nodesDeleted + ', propertiesSet: ' + updateInfo[0].propertiesSet + ', relationshipsCreated: ' + updateInfo[0].relationshipsCreated
          + ', relationshipsDeleted: ' + updateInfo[0].relationshipsDeleted);

        logger.info('labelsAdded: ' + updateInfo[0].labelsAdded + ', labelsRemoved: ' + updateInfo[0].labelsRemoved + ', nodesCreated: ' + updateInfo[0].nodesCreated
          + ', nodesDeleted: ' + updateInfo[0].nodesDeleted + ', propertiesSet: ' + updateInfo[0].propertiesSet + ', relationshipsCreated: ' + updateInfo[0].relationshipsCreated
          + ', relationshipsDeleted: ' + updateInfo[0].relationshipsDeleted);

        loadInreRelCSVInfo.updateMsg = updateMsg;
        driver.close();
        session.close();
      } catch (err) {
        console.log(err);
        logger.info(err);
      }
      console.log('loadInreRelCSVInfo: ' + loadInreRelCSVMsg);
      logger.info('loadInreRelCSVInfo: ' + loadInreRelCSVMsg);
      return resolve(loadInreRelCSVInfo);
    });
  },

  //记录数据更新的信息
  saveContext: async function (id, ctx) {
    try {
      let ctx_id = `ctx_${id}`;
      let res = await redis.set(ctx_id, JSON.stringify(ctx));
      return res;
    } catch (err) {
      console.error('saveContext: ', err);
      logger.error('saveContext: ', err);
      return err;
    }
  },

  //读取数据更新的信息
  getContext: async function (id) {
    try {
      let ctx_id = `ctx_${id}`;
      let res = await redis.get(ctx_id);
      if (res) {
        return JSON.parse(res);
      } else {
        return {};
      }
    } catch (err) {
      console.error('getContext: ', err);
      logger.error('getContext: ', err);
      return err;
    }
  },

  //查询node节点数，可用来检查neo4j server是否正常运行和数据导入成功
  getNodeNum: async function (flag) {
    return new Promise(async (resolve, reject) => {
      let driver = null;
      if (flag == 'a') driver = driver_a;
      else if (flag == 'b') driver = driver_b;
      let session = driver.session();
      let queryBody = `match (n) return count(n)`;
      writeTxPromise = session.writeTransaction(tx => tx.run(queryBody));
      writeTxPromise.then(result => {
        let numRes = result.records[0]._fields[0].low;
        // driver.close();
        session.close();
        return resolve(numRes);
      }).catch(function (error) {
        console.error(error);
        logger.error(error);
        return reject(error);
      });
    });
  },

  //查询node节点数，可用来检查neo4j server是否正常运行和数据导入成功
  getIndexStatus: async function (flag, labelName, indexName) {
    return new Promise(async (resolve, reject) => {
      let driver = null;
      let indexStatus = null;
      if (flag == 'a') driver = driver_a;
      else if (flag == 'b') driver = driver_b;
      let session = driver.session();
      let queryBody = `call db.indexes`;
      writeTxPromise = session.writeTransaction(tx => tx.run(queryBody));
      writeTxPromise.then(result => {
        let indexRecords = result.records;
        for (let subRecord of indexRecords) {
          let indexFields = subRecord._fields;
          let label = indexFields[1];
          let properties = indexFields[2];
          let state = indexFields[3];
          if (label == labelName && properties.indexOf(indexName) != -1) {
            indexStatus = state;
          }
        }
        // driver.close();
        session.close();
        console.log(`index :${labelName}(${indexName}) state: ` + indexStatus);
        logger.info(`index :${labelName}(${indexName}) state: ` + indexStatus);
        return resolve(indexStatus);
      }).catch(function (error) {
        console.error(error);
        logger.error(error);
        return reject(error);
      });
    });
  },

  //查询neo4j sysinfo，可用来检查neo4j server数据导入是否正常
  getSystemInfo: async function (flag) {
    return new Promise(async (resolve, reject) => {
      let driver = null;
      let sysInfo = { nodeIdsNum: 0, propertyIdsNum: 0, relationshipIdsNum: 0, relationshipTypeIdsNum: 0 };
      if (flag == 'a') driver = driver_a;
      else if (flag == 'b') driver = driver_b;
      let session = driver.session();
      let queryBody = "CALL dbms.queryJmx('org.neo4j:instance=kernel#0,name=Primitive count')";
      writeTxPromise = session.writeTransaction(tx => tx.run(queryBody));
      writeTxPromise.then(result => {
        let sysInfoResult = result.records[0]._fields[2];
        sysInfo.nodeIdsNum = sysInfoResult.NumberOfNodeIdsInUse.value.low;
        sysInfo.propertyIdsNum = sysInfoResult.NumberOfPropertyIdsInUse.value.low;
        sysInfo.relationshipIdsNum = sysInfoResult.NumberOfRelationshipIdsInUse.value.low;
        sysInfo.relationshipTypeIdsNum = sysInfoResult.NumberOfRelationshipTypeIdsInUse.value.low;
        session.close();
        return resolve(sysInfo);
      }).catch(function (error) {
        console.error(error);
        logger.error(error);
        return reject(error);
      });
    });
  },

  //redis发布消息(数据更新完后的通知消息)
  publishMessage: async function (message) {
    try {
      let channels = [];
      channels = channels.concat(config.redisPubSubInfo.channelsName);

      for (let channel of channels) {
        pubClient.publish(channel, message);
        console.log('publish the message: %s to channel: %s', message, channel);
        logger.info('publish the message: %s to channel: %s', message, channel);
      }
    } catch (err) {
      console.error('publishMessage',err);
      logger.error('publishMessage',err);
      return err;
    }
  },

  //保存personalCode->ITCode2的字典
  saveHolderDict: async function (key, value) {
    try {
      holder_dict_redis.sadd(key, value);
    } catch (err) {
      console.error('saveHolderDict: ',err);
      logger.error('saveHolderDict: ',err);
      return err;
    }
  },

  //取出personalCode->ITCode2字典数据
  getHolderDict: async function (key) {
    try {
      let res = await holder_dict_redis.smembers(key);
      return res;
    } catch (err) {
      console.error('getHolderDict',err);
      logger.error('getHolderDict',err);
      return err;
    }
  },

  //清空personalCode->ITCode2字典数据
  flushHolderDict: async function() {
    try {
      let res = await holder_dict_redis.flushdb();
      if (res) {
        console.log('flushHolderDict: ', res);
        logger.info('flushHolderDict: ', res);
      }
    } catch (err) {
      console.error('flushHolderDict: ', err);
      logger.error('flushHolderDict: ', err);
      return err;
    }
  }

}


module.exports = transactions;