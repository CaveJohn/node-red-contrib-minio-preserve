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

    function Buckets(config) {
        RED.nodes.createNode(this,config);

        this.name        = config.buckets_name;
        this.operation   = config.buckets_operation;
        this.bucket      = config.buckets_bucket;
        this.region      = config.buckets_region;
        this.prefix      = config.buckets_prefix;
        this.recursive   = config.buckets_recursive;
        this.start_after = config.buckets_start_after;

        var node = this;

        var defaultParams = {
            'bucketName' : node.bucket,
            'region'     : node.region,
            'prefix'     : node.prefix,
            'recursive'  : node.recursive,
            'startAfter' : node.start_after
        }

        // retrive the values from the minio-config node
        node.minioInstance = RED.nodes.getNode(config.host);

        if (node.minioInstance) {
           var minioClient = node.minioInstance.initialize();
        }

        node.status({});

        // TRIGGER ON INCOMING MESSAGE
        node.on('input', function(msg, send, done) {
            var operation = msg.operation || node.operation;
            var opParams = Object.assign({}, defaultParams);
            // If values are provided in the incoming message, then they override those in the node configuration
            opParams.bucketName = msg.bucketName || opParams.bucketName;
            opParams.region     = msg.region || opParams.region;
            opParams.prefix     = msg.prefix || opParams.prefix;
            opParams.recursive  = (typeof msg.recursive === 'boolean') ? msg.recursive : opParams.recursive;
            opParams.startAfter = msg.startAfter || opParams.startAfter;

            var completed = false;
            function finish(output, error) {
                if (completed) { return; }
                completed = true;
                helpers.sendResult(node, msg, output, error, send, done);
            }
            
            // Trigger Bucket Operation type based on "operation" selected in node configuration
            switch (operation) {
                // ====  MAKE BUCKET  ===========================================
                case "makeBucket":
                    helpers.statusUpdate(node, "blue", "dot", 'Making bucket "' + opParams.bucketName + '"');
                    minioClient.makeBucket(opParams.bucketName, opParams.region, function(err) {
                        if (err) {
                            finish({ 'makeBucket': false }, err);
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                        } else {
                            finish({ 'makeBucket': true }, null);
                            helpers.statusUpdate(node, "green", "dot", 'Made bucket "' + opParams.bucketName + '"', 3000);
                        }
                    })
                    break;

                // ====  LIST BUCKETS  ===========================================
                case "listBuckets":
                    helpers.statusUpdate(node, "blue", "dot", 'Listing Buckets');
                    minioClient.listBuckets(function(err, buckets) {
                        if (err) {
                            finish({ 'listBuckets': buckets }, err);
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                        } else {
                            finish({ 'listBuckets': buckets }, null);
                            helpers.statusUpdate(node, "green", "dot", 'Returned ' + buckets.length + ' buckets', 3000);
                        }
                    })
                    break;

                // ====  BUCKET EXISTS  ===========================================
                case "bucketExists":
                    helpers.statusUpdate(node, "blue", "dot", 'Checking if "' + opParams.bucketName + '" exists');
                    minioClient.bucketExists(opParams.bucketName, function(err, exists) {
                        if (err) {
                            finish({ 'bucketExists': false }, err);
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                        } else if (exists) {
                            finish({ 'bucketExists': true }, null);
                            helpers.statusUpdate(node, "green", "dot", 'Bucket "' + opParams.bucketName + '" exists', 3000);
                        } else {
                            finish({ 'bucketExists': false }, null);
                            helpers.statusUpdate(node, "red", "dot", 'Bucket "' + opParams.bucketName + '" doesn\'t exist', 3000);
                        }
                    })                    
                    break;

                // ====  REMOVE BUCKET  ===========================================
                case "removeBucket":
                    helpers.statusUpdate(node, "blue", "dot", 'Removing "' + opParams.bucketName + '" bucket');
                    minioClient.removeBucket(opParams.bucketName, function(err) {
                        if (err) {
                            finish({ 'removeBucket': false }, err);
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                        } else {
                            finish({ 'removeBucket': true }, null);
                            helpers.statusUpdate(node, "green", "dot", 'Bucket "' + opParams.bucketName + '" removed', 3000);
                        };
                    })
                    break;

                // ====  LIST OBJECTS  ===========================================
                case "listObjects":
                    helpers.statusUpdate(node, "blue", "dot", 'Listing objects');
                    var stream = minioClient.listObjects(opParams.bucketName,opParams.prefix, opParams.recursive)
                    var objects = [];
                    stream.on('data',  function(obj) {
                        objects.push(obj);
                    });
                    stream.on('error', function(err) {
                        helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                        finish({ 'listObjects': false }, err);
                    });
                    stream.on('end',   function() { 
                        helpers.statusUpdate(node, "green", "dot", 'Returned ' + objects.length + ' objects', 3000);
                        finish({ 'listObjects': objects }, null);
                    });
                    break;

                // ====  LIST OBJECTS V2  ===========================================
                case "listObjectsV2":
                    helpers.statusUpdate(node, "blue", "dot", 'Listing Objects');
                    var stream = minioClient.listObjectsV2(opParams.bucketName,opParams.prefix, opParams.recursive,opParams.startAfter)
                    var objects = [];
                    stream.on('data',  function(obj) {
                        objects.push(obj);
                    });
                    stream.on('error', function(err) {
                        helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                        finish({ 'listObjectsV2': false }, err);
                    });
                    stream.on('end',   function() { 
                        helpers.statusUpdate(node, "green", "dot", 'Returned ' + objects.length + ' objects', 3000);
                        finish({ 'listObjectsV2': objects }, null);
                    });
                    break;

                // ====  LIST OBJECTS V2 WITH META DATA  ===========================================
                case "listObjectsV2WithMetadata":
                    helpers.statusUpdate(node, "blue", "dot", 'Listing Objects');
                    var stream = minioClient.extensions.listObjectsV2WithMetadata(opParams.bucketName,opParams.prefix, opParams.recursive,opParams.startAfter)
                    var objects = [];
                    stream.on('data',  function(obj) {
                        objects.push(obj);
                    });
                    stream.on('error', function(err) {
                        helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                        finish({ 'listObjectsV2WithMetadata': false }, err);
                    });
                    stream.on('end',   function() { 
                        helpers.statusUpdate(node, "green", "dot", 'Returned ' + objects.length + ' objects', 3000);
                        finish({ 'listObjectsV2WithMetadata': objects }, null);
                    });
                    break;

                // ====  LIST INCOMPLETE UPLOADS  ===========================================
                case "listIncompleteUploads":
                    helpers.statusUpdate(node, "blue", "dot", 'Listing Incomplete Uploads');
                    var stream = minioClient.listIncompleteUploads(opParams.bucketName, opParams.prefix, opParams.recursive)
                    var objects = [];
                    stream.on('data',  function(obj) {
                        objects.push(obj);
                    });
                    stream.on('error', function(err) {
                        helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                        finish({ 'listIncompleteUploads': false }, err);
                    });
                    stream.on('end',   function() { 
                        helpers.statusUpdate(node, "green", "dot", 'Returned ' + objects.length + ' objects', 3000);
                        finish({ 'listIncompleteUploads': objects }, null);
                    });
                    break;

                // ====  DEFAULT - INCORRECT SELECTION   ===========================================
                default:
                    finish(null, 'Invalid Bucket Operation Selection');
            }

        });
        
    }
    RED.nodes.registerType("buckets",Buckets);
}
