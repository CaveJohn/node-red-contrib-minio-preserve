/**
 * Copyright © 2020 Colin Payne.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

module.exports = function (RED) {
    const helpers = require('./helpers');

    function objects(config) {
        RED.nodes.createNode(this, config);

        this.name = config.objects_name;
        this.operation = config.objects_operation;
        this.bucket = config.objects_bucket;
        this.object = config.objects_object;
        this.offset = config.objects_offset;
        this.length = config.objects_length;
        this.stream = config.objects_stream;
        this.size = config.objects_size;
        this.metadata = config.objects_metadata;
        this.sourceobject = config.objects_sourceobject;
        this.conditions = config.objects_conditions;
        this.objectslist = config.objects_objectslist;
        this.prefix = config.objects_prefix;
        this.etag = config.objects_etag;
        this.datetime = config.objects_datetime;

        var node = this;

        var defaultParams = {
            'bucketName': node.bucket,
            'objectName': node.object,
            'offset': node.offset,
            'length': node.length,
            'stream': node.stream,
            'size': node.size,
            'metaData': node.metadata,
            'sourceObject': node.sourceobject,
            'conditions': node.conditions,
            'objectsList': node.objectslist,
            'prefix': node.prefix,
            'etag': node.etag,
            'dateTime': node.datetime
        };

        // retrive the values from the minio-config node
        node.minioInstance = RED.nodes.getNode(config.host);

        if (node.minioInstance) {
            var minioClient = node.minioInstance.initialize();
        }

        // TRIGGER ON INCOMING MESSAGE
        node.on('input', function (msg, send, done) {
            var operation = msg.operation || node.operation;
            var opParams = Object.assign({}, defaultParams);
            // If values are provided in the incoming message, then they override those set in the node configuration
            opParams.bucketName = msg.bucketName || opParams.bucketName;
            opParams.objectName = msg.objectName || opParams.objectName;
            opParams.offset = msg.offset || opParams.offset;
            opParams.length = msg.length || opParams.length;
            opParams.stream = msg.stream || opParams.stream;
            opParams.size = msg.size || opParams.size;
            opParams.metaData = msg.metaData || opParams.metaData;
            opParams.sourceObject = msg.sourceObject || opParams.sourceObject;
            opParams.conditions = msg.conditions || opParams.conditions;
            opParams.objectsList = msg.objectsList || opParams.objectsList;
            opParams.prefix = msg.prefix || opParams.prefix;
            opParams.etag = msg.etag || opParams.etag;
            opParams.dateTime = msg.dateTime || opParams.dateTime;

            var completed = false;
            function finish(output, error) {
                if (completed) { return; }
                completed = true;
                helpers.sendResult(node, msg, output, error, send, done);
            }

            // Trigger Bucket Operation type based on "operation" selected in node configuration
            switch (operation) {

                // ====  GET OBJECT  ===================================================
                case "getObject":
                    var size = 0,
                        fileData = [];

                    minioClient.getObject(opParams.bucketName, opParams.objectName, function (err, dataStream) {
                        try {
                            if (err) {
                                helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                                finish({ 'getObject': false }, err);
                                return;
                            }
                            dataStream.on('data', function (chunk) {
                                size += chunk.length;
                                fileData.push(chunk);
                            });
                            dataStream.on('end', function () {
                                var objectData = Buffer.concat(fileData);
                                finish({
                                    'getObject': true,
                                    'objectData': objectData,
                                    'opjectSize': size
                                }, null);
                            });
                            dataStream.on('error', function (err) {
                                helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                                finish({ 'getObject': false }, err);
                            });
                        } catch (e) {
                            finish({ 'getObject': false }, e);
                        }
                    });
                    break;

                    // ====  GET PARTIAL OBJECT  ===========================================
                case "getPartialObject":
                    var psize = 0;
                    minioClient.getPartialObject(opParams.bucketName, opParams.objectName, parseInt(opParams.offset), parseInt(opParams.length), function (err, dataStream) {
                        helpers.statusUpdate(node, "blue", "dot", 'Attempting getPartialObject...', 5000);
                        if (err) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            finish({ 'getPartialObject': false }, err);
                            return;
                        }
                        var receivedChunk;
                        dataStream.on('data', function (chunk) {
                            helpers.statusUpdate(node, "blue", "dot", 'Receiving data...', 5000);
                            psize += chunk.length;
                            receivedChunk = chunk;
                        });
                        dataStream.on('end', function () {
                            helpers.statusUpdate(node, "green", "dot", 'Object Chunk Received', 5000);
                            finish({
                                'getPartialObject': true,
                                'chunk': receivedChunk
                            }, null);
                        });
                        dataStream.on('error', function (err) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            finish({ 'getPartialObject': false }, err);
                        });
                    });
                    break;

                    // ====  PUT OBJECT  ===================================================
                case "putObject":
                    minioClient.putObject(opParams.bucketName, opParams.objectName, opParams.stream, function (err, etag) {
                        if (err) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            finish({ 'putObject': false }, err);
                        } else {
                            finish({
                                'putObject': true,
                                'etag': etag
                            }, null);
                        }
                    });
                    break;

                    // ====  COPY OBJECT  ==================================================
                case "copyObject":
                    var Minio = require('minio');
                    var conds = new Minio.CopyConditions();
                    console.log('opParams.conditions', opParams.conditions);
                    switch (opParams.conditions) {
                        case "setMatchETag":
                            console.log('opParams.etag', opParams.etag);
                            conds.setMatchETag(opParams.etag);
                            break;
                        case "setMatchETagExcept":
                            console.log('opParams.etag', opParams.etag);
                            conds.setMatchETagExcept(opParams.etag);
                            break;
                        case "setModified":
                            var d = new Date(opParams.dateTime);
                            console.log('opParams.dateTime', d);
                            conds.setModified(d);
                            break;
                        case "setReplaceMetadataDirective":
                            conds.setReplaceMetadataDirective();
                            break;
                        case "setUnmodified":
                            console.log('opParams.dateTime', opParams.dateTime);
                            conds.setUnmodified(opParams.dateTime);
                            break;
                        case "copyDefault":
                            break;
                    }

                    console.log('conds', conds);

                    minioClient.copyObject(opParams.bucketName, opParams.objectName, opParams.sourceObject, conds, function (err, data) {
                        if (err) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            finish({ 'copyObject': false }, err);
                        } else {
                            helpers.statusUpdate(node, "green", "dot", 'Object Copied', 5000);
                            finish({
                                'copyObject': true,
                                'etag': data.etag,
                                'lastModified': data.lastModified
                            }, null);
                        }
                    });
                    break;

                    // ====  STAT OBJECT  ==================================================
                case "statObject":
                    minioClient.statObject(opParams.bucketName, opParams.objectName, function (err, stat) {
                        if (err) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            finish({ 'statObject': false }, err);
                        } else {
                            finish({
                                'statObject': true,
                                'stat': stat
                            }, null);
                        }
                    });
                    break;

                    // ====  REMOVE OBJECT  ================================================
                case "removeObject":
                    minioClient.removeObject(opParams.bucketName, opParams.objectName, function (err) {
                        if (err) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            finish({ 'removeObject': false }, err);
                        } else {
                            finish({ 'removeObject': true }, null);
                        }
                    });
                    break;

                    // ====  REMOVE OBJECTS  ===============================================
                case "removeObjects":
                    if (opParams.objectsList) {
                        minioClient.removeObjects(opParams.bucketName, JSON.parse(opParams.objectsList), function (err) {
                            if (err) {
                                helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                                finish({ 'removeObjects': false }, err);
                            } else {
                                finish({ 'removeObjects': true }, null);
                            }
                        });
                    } else {
                        var objectsList = [];

                        var objectsStream = minioClient.listObjects(opParams.bucketName, opParams.prefix, true);

                        objectsStream.on('data', function (obj) {
                            objectsList.push(obj.name);
                        });

                        objectsStream.on('error', function (e) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            finish({ 'removeObjects': false }, e);
                        });

                        objectsStream.on('end', function () {
                            minioClient.removeObjects(opParams.bucketName, objectsList, function (err) {
                                if (err) {
                                    helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                                    finish({ 'removeObjects': false }, err);
                                } else {
                                    finish({ 'removeObjects': true }, null);
                                }
                            });
                        });
                    }
                    break;

                    // ====  REMOVE INCOMPLETE UPLOAD  =====================================
                case "removeIncompleteUpload":
                    // LIFTED STRAIGHT FROM SPEC - TO BE COMPLETED:
                    minioClient.removeIncompleteUpload(opParams.bucketName, opParams.objectName, function (err) {
                        if (err) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            return finish({ 'removeIncompleteUpload': false }, err);
                        }
                        finish({ 'removeIncompleteUpload': true }, null);
                    });
                    break;

                    // ====  DEFAULT - INCORRECT SELECTION   ===============================
                default:
                    finish(null, 'Invalid File Object Operation Selection');
            }

        });

    }
    RED.nodes.registerType("objects", objects);
};
