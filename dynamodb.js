"use strict";
Object.defineProperty(exports, "__esModule", { value: true });

/* DB Connection */

const sdk = require('aws-sdk');
const config = require('./config');

sdk.config.credentials = new sdk.Credentials
  (
    process.env.AWS_ACCESS_KEY_ID,
    process.env.AWS_ACCESS_KEY_SECRET
  );

sdk.config.update({ region: process.env.AWS_REGION });

const dbClient = new sdk.DynamoDB();

/* ! DB Connection */

class DynamoStorage {
    constructor(table = 'AzureBOTSession') {
        this.table = table;
        this.etag = 1;
		this.cache = {};
    }
    read(keys) {
        return new Promise((resolve, reject) => {
			
			var batchRequest = [];
			
			const data = {};
			
			console.log(keys);
			for(var i = 0; i < keys.length; i++)
			{
				if(this.cache[keys[i]] !== undefined)
					data[keys[i]] = this.cache[keys[i]];
				else
					batchRequest.push({ID: {S: keys[i]}});
			}
			
			var param = {RequestItems: {}, ReturnConsumedCapacity: 'NONE'};
			param.RequestItems[this.table] = {Keys: batchRequest};
			
			if(batchRequest.length > 0)
			{
				dbClient.batchGetItem(param, (err, res) => { 
					if(err)
						reject(err);
					else
					{
						
						
						if(typeof res.Responses[this.table] !== 'undefined' && res.Responses[this.table].length > 0)
						{
							for(var i = 0; i < res.Responses[this.table].length; i++)
							{
								data[res.Responses[this.table][i].ID.S] = JSON.parse(res.Responses[this.table][i].Value.S);
							}
						}
						
						resolve(data);
					}
				});
			}
			else
				resolve(data);
        });
    }
    ;
    write(changes) {
		for (const key in changes) {
				this.cache[key] = changes[key];
			 }
        return new Promise((resolve, reject) => {
			
			var batch = {RequestItems: {}};
			batch.RequestItems[this.table] = [];
			
			 for (const key in changes) {
				batch.RequestItems[this.table].push({PutRequest: {Item: { ID: {S: key}, Value: {S: JSON.stringify(changes[key])}, expireAt: {N: Math.round(new Date().getTime() / 1000 + 3600 * 4).toString()}}}});
			 }
			
			dbClient.batchWriteItem(batch, (err, res) => {
				if(err)
					reject(err);
				else
					resolve();
				});
           
        });
    }
    ;
    delete(keys) {
		for(var i = 0; i< keys.length;i++)
				delete this.cache[keys[i]];
			
        return new Promise((resolve, reject) => {

			var batch = {RequestItems: {}};
			batch.RequestItems[this.table] = [];
			
			 for (const key in changes) {
				batch.RequestItems[this.table].push({DeleteRequest: {Key: { ID: {S: key}}}});
			 }
			
			dbClient.batchWriteItem(batch, (err, res) => {
				if(err)
					reject(err);
				else
					resolve();
				});
           
        });
    }
}
exports.DynamoStorage = DynamoStorage;