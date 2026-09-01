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

module.exports = function(RED) {
    const helpers = require('./helpers');

    function files(config) {
        RED.nodes.createNode(this,config);

        this.name      = config.files_name;
        this.operation = config.files_operation;
        this.bucket    = config.files_bucket;
        this.object    = config.files_object;
        this.file_path = config.files_filepath;
        this.meta_data = config.files_metadata;

        var node = this;

        var defaultParams = {
            'bucketName' : node.bucket,
            'objectName' : node.object,
            'filePath'   : node.file_path,
            'metaData'   : node.meta_data
        }

        // retrive the values from the minio-config node
        node.minioInstance = RED.nodes.getNode(config.host);

        if (node.minioInstance) {
            var minioClient = node.minioInstance.initialize();
        }
 
        // TRIGGER ON INCOMING MESSAGE
        node.on('input', function(msg, send, done) {
            // Keep all request state local. Node instances process more than one
            // message concurrently, so node-level output/parameter state races.
            var operation = msg.operation || node.operation;
            var opParams = Object.assign({}, defaultParams);
            // If values are provided in the incoming message, then they override those set in the node configuration
            opParams.bucketName = msg.bucketName || opParams.bucketName;
            opParams.objectName = msg.objectName || opParams.objectName;
            opParams.filePath = msg.filePath || opParams.filePath;
            opParams.metaData = msg.metaData || opParams.metaData;

            function finish(output, error) {
                helpers.sendResult(node, msg, output, error, send, done);
            }
            
            // Trigger Bucket Operation type based on "operation" selected in node configuration
            switch (operation) {
                
                // ====  FILE GET OBJECT  ===========================================
                case "fGetObject":
                    helpers.statusUpdate(node, "blue", "dot", 'Getting object "' + opParams.objectName + '"');
                    minioClient.fGetObject(opParams.bucketName, opParams.objectName, opParams.filePath, function(err) {
                        if (err) {
                            finish({ 'fGetObject': false }, err);
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                        } else {
                            finish({ 'fGetObject': true }, null);
                            helpers.statusUpdate(node, "green", "dot", 'Get object "' + opParams.objectName + '" successful', 3000);
                        }
                    })
                    break;

                // ====  FILE PUT OBJECT  ===========================================
                case "fPutObject":
                    if (opParams.metaData) {
                        helpers.statusUpdate(node, "blue", "dot", 'Putting object "' + opParams.objectName + '"');
                        minioClient.fPutObject(opParams.bucketName, opParams.objectName, opParams.filePath, opParams.metaData, function(err, etag) {
                            if (err) {
                                finish({ 'fPutObject': false }, err);
                                helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            } else {
                                finish({ 'fPutObject': true, 'etag': etag }, null);
                                helpers.statusUpdate(node, "green", "dot", 'Put object "' + opParams.objectName + '" successful', 3000);
                            }
                        })
                    } else {
                        helpers.statusUpdate(node, "blue", "dot", 'Putting object "' + opParams.objectName + '"');
                        minioClient.fPutObject(opParams.bucketName, opParams.objectName, opParams.filePath, function(err, etag) {
                            if (err) {
                                finish({ 'fPutObject': false }, err);
                                helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            } else {
                                finish({ 'fPutObject': true, 'etag': etag }, null);
                                helpers.statusUpdate(node, "green", "dot", 'Put object "' + opParams.objectName + '" successful', 3000);
                            }
                        })
                    }
                    break;

                // ====  DEFAULT - INCORRECT SELECTION   ===========================================
                default:
                    finish(null, 'Invalid File Object Operation Selection');
            }

        });
        
    }
    RED.nodes.registerType("files",files);
}
