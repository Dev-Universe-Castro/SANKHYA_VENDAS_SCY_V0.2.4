
const http = require('http');

const options = {
    hostname: 'localhost',
    port: 4000,
    path: '/api/sync/queue',
    method: 'GET'
};

const req = http.request(options, res => {
    let data = '';
    res.on('data', chunk => {
        data += chunk;
    });
    res.on('end', () => {
        console.log(data);
    });
});

req.on('error', error => {
    console.error('Erro:', error.message);
});

req.end();
