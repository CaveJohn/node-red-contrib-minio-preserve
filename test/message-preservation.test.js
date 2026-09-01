const assert = require('assert');

function loadNode(modulePath, type, config, client) {
    let constructor;
    const RED = {
        nodes: {
            createNode(node) {
                node.on = function(event, handler) {
                    if (event === 'input') {
                        node.input = handler;
                    }
                };
                node.status = function() {};
                node.send = function() {};
            },
            getNode() {
                return { initialize: () => client };
            },
            registerType(name, nodeConstructor) {
                if (name === type) {
                    constructor = nodeConstructor;
                }
            }
        }
    };

    require(modulePath)(RED);
    const node = {};
    constructor.call(node, config);
    return node;
}

function call(node, msg) {
    return new Promise((resolve) => {
        node.input(msg, (outputs) => resolve(outputs), () => {});
    });
}

async function preservesOriginalMessageAcrossConcurrentRequests() {
    const client = {
        fPutObject(bucket, object, filePath, callback) {
            setTimeout(() => callback(null, 'etag-' + object), object === 'slow' ? 25 : 5);
        }
    };
    const node = loadNode('../files.js', 'files', {
        files_operation: 'fPutObject', files_bucket: 'configured', files_object: 'configured', files_filepath: '/tmp/configured'
    }, client);
    const slow = { _id: 'slow-id', req: { requestId: 'slow-request' }, objectName: 'slow', filePath: '/tmp/slow' };
    const fast = { _id: 'fast-id', req: { requestId: 'fast-request' }, objectName: 'fast', filePath: '/tmp/fast' };

    const [slowOutputs, fastOutputs] = await Promise.all([call(node, slow), call(node, fast)]);

    assert.strictEqual(slowOutputs[0], slow);
    assert.strictEqual(fastOutputs[0], fast);
    assert.deepStrictEqual(slow.payload, { fPutObject: true, etag: 'etag-slow' });
    assert.deepStrictEqual(fast.payload, { fPutObject: true, etag: 'etag-fast' });
    assert.strictEqual(slow.req.requestId, 'slow-request');
    assert.strictEqual(fast.req.requestId, 'fast-request');
    assert.strictEqual(slowOutputs[1], null);
    assert.strictEqual(fastOutputs[1], null);
}

async function keepsOriginOnBothErrorOutputs() {
    const expectedError = new Error('not found');
    const client = {
        getBucketPolicy(bucket, callback) {
            setTimeout(() => callback(expectedError), 1);
        }
    };
    const node = loadNode('../policies.js', 'policies', {
        policies_operation: 'getBucketPolicy', policies_bucket: 'configured'
    }, client);
    const msg = { _id: 'failure-id', req: { requestId: 'failure-request' } };
    const outputs = await call(node, msg);

    assert.strictEqual(outputs[0], msg);
    assert.deepStrictEqual(msg.payload, { getBucketPolicy: false });
    assert.notStrictEqual(outputs[1], msg);
    assert.strictEqual(outputs[1]._id, 'failure-id');
    assert.strictEqual(outputs[1].req, msg.req);
    assert.strictEqual(outputs[1].payload, expectedError);
}

async function main() {
    await preservesOriginalMessageAcrossConcurrentRequests();
    await keepsOriginOnBothErrorOutputs();
    console.log('message preservation tests passed');
}

main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
