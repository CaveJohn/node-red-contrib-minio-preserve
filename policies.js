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

    function policies(config) {
        RED.nodes.createNode(this,config);

        this.name      = config.policies_name;
        this.operation = config.policies_operation;
        this.bucket    = config.policies_bucket;
        this.policy    = config.policies_policy;

        var node = this;

        var defaultParams = {
            'bucketName'   : node.bucket,
            'bucketPolicy' : node.policy,
        }

        // retrive the values from the minio-config node
        node.minioInstance = RED.nodes.getNode(config.host);

        if (node.minioInstance) {
            var minioClient = node.minioInstance.initialize();
        }
 
        // TRIGGER ON INCOMING MESSAGE
        node.on('input', function(msg, send, done) {
            var operation = msg.operation || node.operation;
            var opParams = Object.assign({}, defaultParams);
            // If values are provided in the incoming message, then they override those set in the node configuration
            opParams.bucketName   = msg.bucketName || opParams.bucketName;
            opParams.bucketPolicy = msg.bucketPolicy || opParams.bucketPolicy;

            function finish(output, error) {
                helpers.sendResult(node, msg, output, error, send, done);
            }
            
            // Trigger Bucket Operation type based on "operation" selected in node configuration
            switch (operation) {
                
                // ====  GET BUCKET POLICY  ===================================================
                case "getBucketPolicy":
                    helpers.statusUpdate(node, "blue", "dot", 'Fetching Bucket Policy...');
                    minioClient.getBucketPolicy(opParams.bucketName, function(err, policy) {
                        if (err) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            finish({ 'getBucketPolicy': false }, err);
                        } else {
                            helpers.statusUpdate(node, "green", "dot", 'Returned Bucket Policy', 5000);
                            finish({
                                'getBucketPolicy': true,
                                'policy': policy
                            }, null);
                        }
                    })
                    
                    break;

                // ====  SET BUCKET POLICY  ===========================================
                case "setBucketPolicy":
                    helpers.statusUpdate(node, "blue", "dot", 'Setting Bucket Policy...');
                    minioClient.setBucketPolicy(opParams.bucketName, JSON.stringify(opParams.bucketPolicy), function(err) {
                        if (err) {
                            helpers.statusUpdate(node, "red", "dot", 'Error', 5000);
                            finish({ 'setBucketPolicy': false }, err);
                        } else {
                            helpers.statusUpdate(node, "green", "dot", 'Set Bucket Policy', 5000);
                            finish({ 'setBucketPolicy': true }, null);
                        }
                    })

                    break;

                // ====  DEFAULT - INCORRECT SELECTION   ===============================
                default:
                    finish(null, 'Invalid File Object Operation Selection');
            }

        });
        
    }
    RED.nodes.registerType("policies",policies);
}
