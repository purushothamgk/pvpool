/* eslint-disable no-constant-condition */
const express = require('express')
const app = express()
const du = require('diskusage');
const os = require('os')
const fs = require('fs').promises
const path = require('path')

const port = process.env.PORT || 8080;
const pv_path = process.env.PV_PATH || path.join(__dirname, '.data');
const usage_file_path = path.join(pv_path, 'usage');

const INIT_DELAY = 60000;
const LOOP_DELAY = 10000;

const UNKNOWN_STATE = 'unknown';
const INIT_STATE = 'Initializing';
const READY_STATE = 'Ready';
const DECOMMISSIONING_STATE = 'Decommissioning';
const DECOMMISSIONED_STATE = 'Decommissioned';

const status = {
    name: os.hostname(),
    total: 0,
    used: 0,
    state: UNKNOWN_STATE
}



const logger = {
    log: (...args) => console.log(`[${process.pid}]`, new Date(), ...args),
    error: (...args) => console.error(`[${process.pid}]`, new Date, ...args),
}

app.get('/', (req, res) => {
    res.send(`Hello from storage-agent ${status.name}`)
})

app.get('/status', (req, res) => {
    res.json(status).end()
})

app.put('/manage-agent/:op', async (req, res) => {
    if (req.params.op === 'decommission') {
        // decommission agent
        logger.log(`got request to decommission storage-agent ${status.name}`)
        status.state = DECOMMISSIONING_STATE;
        await write_status();
        res.end();
    } else {
        res.status(403);
    }
})


app.listen(port, () => {
    logger.log(`storage-agent ${status.name} listening at http://localhost:${port}`)
})


function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function write_status() {
    await fs.writeFile(usage_file_path, JSON.stringify(status));
    logger.log(`storage status =`, JSON.stringify(status));
}

async function delete_status_file() {
    await fs.unlink(usage_file_path)
    logger.log('status file deleted');
}

function panic(message, err) {
    logger.error('PANIC!!', message, err, err.stack);
    process.exit(1);
}

async function main() {
    try {
        const {
            total
        } = await du.check(pv_path);
        status.total = total;
        let stored_status;
        try {
            stored_status = JSON.parse(await fs.readFile(usage_file_path));
            status.used = stored_status.used;
            status.state = stored_status.state;
        } catch (err) {
            if (err.code === 'ENOENT') {
                // initializing state for one minute
                status.state = INIT_STATE;
                await delay(INIT_DELAY);
                // move to ready and store status
                status.state = READY_STATE;
                await write_status();
            } else {
                panic('unknown error', err);
            }
        }

        // status update loop
        while (true) {
            if (status.state === READY_STATE) {
                // size is between -1 (deletion) and 3 MB. avg of 1MB for iteration 
                const rand_buffer_size_bytes = Math.floor((Math.random() * 4 - 1) * 1024 * 1024);
                const new_used = status.used + rand_buffer_size_bytes;
                if (new_used > 0 && new_used < status.total) {
                    // if new_used  is valid update new size and sleep
                    status.used = new_used;
                }
                await write_status();
            } else if (status.state === DECOMMISSIONING_STATE) {
                logger.log('decommissioning agent');
                await delay(60000)
                status.state = DECOMMISSIONED_STATE;
                status.used = 0;
                // delete the status file. on restart will start as a new agent
                await delete_status_file();
            } else if (status.state === DECOMMISSIONED_STATE) {
                logger.log('storage-agent is decommissioned');
            }
            await delay(LOOP_DELAY);
        }
    } catch (err) {
        panic('unknown error', err);
    }
}


if (require.main === module) {
    main();
}