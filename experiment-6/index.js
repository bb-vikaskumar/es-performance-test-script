"use strict";

/*
    This is a test to compare performance of ES in below 2 scenarios:
        1. Keep CET in one index. Split 3 Lakh Mid into docs with 50k in another index.
        2. Merge results from both indices. 
    
    No. of Docs in index: 5k
    Total running time: 30 minutes
    Final Results can be seen here: https://docs.google.com/spreadsheets/d/1uINIX6-0XB6LGJKuBN1GwLSEFb54NBx2pYew4d0oVLM/edit#gid=0
*/

let elasticsearch = require('elasticsearch');
let _ = require('lodash');
const fs = require('fs')
  
const ES_INDEX_NAME = "only_mids";
const ELASTICSEARCH_IP = "https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com";
const ELASTICSEARCH_PORT = 443;

const ACTIVITY_TYPE = 'fetch'; // create, fetch
const TOTAL_DOCS_COUNT = 5;
const FETCH_TYPE='time'  // time, count
const BATCH_SIZE = 5;
const SEARCH_DURATION_IN_MINS = 0.01;
const MIDS_COUNT_PER_DOC = 10;
const MIDS_SPACE_COUNT = MIDS_COUNT_PER_DOC * 30;
const LOG_MOD = 500; // every LOD_MOD record will get logged
const SEIGE_URLS_TO_KEEP = 3000;
const ES_INDEX_FINAL = ES_INDEX_NAME;

let savedRecordFile = `./saved_docs.json`;



let esClient = new elasticsearch.Client({
    host: ELASTICSEARCH_IP
});


class ElasticSearchConnector {

    static ping() {

        return new Promise(_ping);
        function _ping(resolve, reject) {

            esClient.ping({
                requestTimeout:3000,
            })
                .then(function(res) {
                    return resolve("Elastic Search Cluster is Up");
                })
                .catch(function(err) {

                    return reject(err)
                });
        }
    }

    static addDocument(indexName, _id, payload, docType) {

        return new Promise(_addDocument);
        function _addDocument(resolve, reject) {

            esClient.index({
                index: indexName,
                type: docType || "_doc",
                id: _id,
                body: payload
            })
                .then(function (res) {
                    console.log('** added res: ', res);
                    return resolve({"campaign_id":_id,"res":res,"payload":payload});
                })
                .catch(function (err)  {
                    console.log('** error while adding document: ', err);
                    return reject(err);
                });
        }
    }

    static search(indexName, payload, docType, from, size) {
        let searchQuery = {
            index: indexName,
            preference: '_local',
            type: docType || "_doc",
            body: payload
        }
        if(from != undefined) {
            searchQuery['from'] = from;
        }
        if(size) {
            searchQuery['size'] = size;
        }

        return new Promise(_search);
        function _search(resolve, reject) {

            esClient.search(searchQuery)
                .then(function (res) {
                    console.log('** search res: ', JSON.stringify(res));
                    return resolve(res);
                })
                .catch(function (err)  {
                    console.log('** error while searching: ', err);
                    return reject(err);
                });
        }
    }
}


function generateSeries({from, count}) {
    let series = [];
    for(let i=from; i<from+count; i++) {
        series.push(i);
    }
    return series;
}

function generateRandomNumbers({from, to, count}) {
    let nums = [];
    for(let i=0; i<count; i++) {
        nums.push(from + Math.floor(Math.random() * (to-from)));   
    }
    return nums;
}

function generateEmails({count}) {
    let emails = [];
    for(let i=0; i<count; i++) {
        let initialChars = 'abcdefghijklmnopqrstuvwxyz';
        let firstChar = initialChars[Math.floor(Math.random() * initialChars.length)];

        let chars = 'abcdefghijklmnopqrstuvwxyz1234567890';
        let email = firstChar;
        for(let j=0; j<10; j++){
            email += chars[Math.floor(Math.random() * chars.length)];
        }
        email = email + '@gmail.com';
        emails.push(email);
    }
    return emails;
}

function generateStrings({count}) {
    let strings = [];
    for(let i=0; i<count; i++) {
        let chars = 'abcdefghijklmnopqrstuvwxyz';
        let str = '';
        for(let j=0; j<5; j++){
            str += chars[Math.floor(Math.random() * chars.length)];
        }
        strings.push(str);
    }
    return strings;
}

function generatePhoneNumbers({count}) {
    let phones = [];
    for(let i=0; i<count; i++) {
        let initialChars = '123456789';
        let firstNum = initialChars[Math.floor(Math.random() * initialChars.length)];

        let chars = '1234567890';
        let phone = firstNum;
        for(let j=0; j<9; j++){
            phone += chars[Math.floor(Math.random() * chars.length)];
        }
        phones.push(phone);
    }
    return phones;
}

function generateCombination({list, size}) {
    if(!list || !list.length) {
        return [];
    }
    if(size === list.length) {
        return list;
    }
    let comb = [];
    for(let j=0; j<size; j++){
        comb.push(list[Math.floor(Math.random() * list.length)]);
    }
    return [...new Set(comb)];
}

function pickOne({list}) {
    return list[Math.floor(Math.random() * list.length)];
}

function generatePermutedDoc({sourceSpace, addMid}) {
    let doc = {
        "mid": generateCombination({list: sourceSpace.mid, size: MIDS_COUNT_PER_DOC})
    };
    return doc;
}

function generateMids({sourceSpace, size, addAll}) {
    let mids = generateCombination({list: sourceSpace.mid, size: size});
    if(addAll) {
        mids = [...new Set(['all', ...mids])];
    }
    return mids;
}

function generatePermutedDocs({sourceSpace, count}) {
    let docs = [];
    for(let i=0; i<count; i++) {
        let doc = generatePermutedDoc({sourceSpace, index: i});
        docs.push(doc);
    }
    return docs;
}

function demoPromise({returnVal, delay}) {
    return new Promise(function(resolve) {
        setTimeout(() => resolve(returnVal), delay);
    });
}

function processBatch({docs, from, batchId, batchSize, esIndex=ES_INDEX_NAME}) {
    let totalDocsCount = docs.length;
    from = from % totalDocsCount;
    let to = from+batchSize;
    let batchDocs = docs.slice(from, to);
    if(to > docs.length) {
        batchDocs = batchDocs.concat(docs.slice(0, to-totalDocsCount))
    }
    return Promise.all(_.map(batchDocs, (doc, docIndex) => {
        let docId = (batchId * batchSize) + docIndex;
        if(ACTIVITY_TYPE === 'create') {
            console.log('write on ', esIndex, ' : ', JSON.stringify(doc));
            return demoPromise({returnVal: 'processed '+ docId+ '_'+ batchId, delay: 2000});
            return ElasticSearchConnector.addDocument(esIndex, docId, doc);
        } else if(ACTIVITY_TYPE === 'fetch') {
            console.log('search on ', esIndex, ' : ', JSON.stringify(doc));
            return demoPromise({returnVal: 'processed '+ docId+ '_'+ batchId, delay: 2000});
            return ElasticSearchConnector.search(esIndex, doc);
        } else {
            return demoPromise({returnVal: 'processed '+ docId+ '_'+ batchId, delay: 1000});
        } 
    }))
}

let batchResults = {};

async function processAllBatches({docs, batchSize, esIndex}) {
    let batchId;
    let startTime;
    try {

        let totalDocsCount = docs.length;
        
        if(ACTIVITY_TYPE === 'fetch' && FETCH_TYPE === 'time') {
            let targetTime = new Date().getTime() + SEARCH_DURATION_IN_MINS * 60 * 1000;
            batchId = 0;

            while(targetTime > new Date().getTime()) {
                let from = batchId * batchSize;
                startTime = new Date();
                let processedAck = await processBatch({docs, from, batchId, batchSize, esIndex: esIndex});

                if(batchId % LOG_MOD === 0) {
                    batchResults[batchId] = {
                        timeElapsed: `${new Date() - startTime}ms`,
                        totalDocsCount: totalDocsCount,
                        from: from,
                        batchSize: batchSize                     
                    }
                    console.log(`[success] time batch: ${batchId} | ${new Date() - startTime}ms`);
                }
                batchId++;
            }



        } else {
            for(batchId=0; (batchId*batchSize)<totalDocsCount; batchId++) {
                let from = batchId * batchSize;
                startTime = new Date();
    
                let processedAck = await processBatch({docs, from, batchId, batchSize, esIndex: esIndex});
    
                if(batchId % LOG_MOD === 0) {
                    batchResults[batchId] = {
                        timeElapsed: `${new Date() - startTime}ms`,
                        totalDocsCount: totalDocsCount,
                        from: from,
                        batchSize: batchSize                     
                    }
                    console.log(`[success] batch: ${batchId} | ${new Date() - startTime}ms`);
                }
            }
        }
        
    } catch(e) {
        batchResults[batchId] = {
            error: e.stack,
            totalDocsCount: docs.length,
            batchId: batchId,
            batchSize: batchSize                     
        }
        console.log(`[error] batch: ${batchId} | ${new Date() - startTime}ms, error: ${e.stack}`);
        reject(e);
    }
}

function generateFetchQueries({docs, termsCount}) {
    let queries = [];
    
    for(let doc of docs) {
        let query = {
            'query': {
                'bool': {
                    'filter': []
                }
            },
            "_source": false
        }
        for(let field of Object.keys(doc)) {
            let values;

            values = generateCombination({list: doc[field], size: termsCount[field]});
            let terms = {
                [field]: values
            };

            query.query.bool.filter.push({'terms': terms});
        }
        queries.push(query);
    }

    return queries;
}


async function createRecords({docCount, batchSize}) {
    let sourceSpace = {
        mid: ['all', ...generateSeries({from: 1, count: MIDS_SPACE_COUNT})]
    }

    console.log(`Generating records from space...`);
    
    let allDocs = generatePermutedDocs({sourceSpace: sourceSpace, count: docCount});
    console.log(`Record generation success.`);
    

    let batchId;
    let totalDocsCount = TOTAL_DOCS_COUNT;
    let startTime = new Date();

    try {
        for(let i=0; i<docCount; i++) {
            let docWithMid = allDocs[i];
            let midCount = MIDS_COUNT_PER_DOC;
            docWithMid['mid'] = generateMids({sourceSpace: sourceSpace, size: midCount, addAll: true});
            
            console.log(`[${i}] Record generation success (with ${midCount} MID).`);
            
            batchId = i;
            batchSize = 1;
            let from = 0;
            startTime = new Date();

            let processedAck = await processBatch({docs: [docWithMid], from, batchId, batchSize, esIndex: ES_INDEX_NAME});

            if(batchId % LOG_MOD === 0) {
                batchResults[batchId] = {
                    timeElapsed: `${new Date() - startTime}ms`,
                    totalDocsCount: totalDocsCount,
                    from: from,
                    batchSize: batchSize                     
                }
                console.log(`[success] batch: ${batchId} | ${new Date() - startTime}ms`);
            }

            
            allDocs[i]['mid'] = docWithMid['mid'].slice(1, 5);
        }
        // Write data in 'Output.txt' .
        fs.writeFile(`saved_docs.json`, JSON.stringify({records: allDocs}), (err) => {
            if (err) throw err;
            console.log(`Records(${allDocs.length}) written to saved_docs.json`);
        })
            
    } catch(e) {
        batchResults[batchId] = {
            error: e.stack,
            totalDocsCount: totalDocsCount,
            batchId: batchId,
            batchSize: batchSize                     
        }
        console.log(`[error] batch (with mid): ${batchId} | ${new Date() - startTime}ms, error: ${e.stack}`);
    }
}

async function fetchRecords({docCount, batchSize}) {
    let termsCount = {
        mid: 1
    };

    console.log('Reading records from ', savedRecordFile);
    let savedDocs = require(savedRecordFile).records;
    let allDocs = [];
    for(let i=0; i<docCount; i++) {
       allDocs.push(savedDocs[i%savedDocs.length]);
    }

    let allQueries = generateFetchQueries({docs: allDocs, termsCount: termsCount});
    
    // let siegeUrls = [];
    // for(let query of allQueries.slice(0, SEIGE_URLS_TO_KEEP)) {
    //     let siegeUrl = `${ELASTICSEARCH_IP}/${ES_INDEX_FINAL}/_search POST ${JSON.stringify(query)}`;
    //     siegeUrls.push(siegeUrl);
    // }

    // fs.writeFile(`siegeUrls${WITH_MID ? '_withMid': ''}.txt`, siegeUrls.join('\n'), (err) => {
    //     if (err) throw err;
    // })

    let startTime = new Date();
    processAllBatches({docs: allQueries, from: 0, batchId: 0, batchSize: batchSize, esIndex: ES_INDEX_FINAL})
        .then((res) => { 
            console.log({batchResults: batchResults, totalReadTime: `${new Date() - startTime}ms`})
        })
        .catch((e) => {
            console.log({batchResults: batchResults, totalReadTime: `${new Date() - startTime}ms`, error: e});
        });
}

if(ACTIVITY_TYPE === 'create') {
    createRecords({docCount:TOTAL_DOCS_COUNT, batchSize: BATCH_SIZE});
} else if (ACTIVITY_TYPE === 'fetch') {
    fetchRecords({docCount:TOTAL_DOCS_COUNT, batchSize: BATCH_SIZE});
}